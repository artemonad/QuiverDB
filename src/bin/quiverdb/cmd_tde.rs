use anyhow::{Context, Result};
use rand::rngs::OsRng;
use rand::RngCore;
use std::path::PathBuf;

use QuiverDB::crypto::{EnvKeyProvider, KeyJournal, KeyProvider}; // <- добавлен KeyProvider
use QuiverDB::crypto::{EnvKmsProvider, KeyRing, KmsProvider};
use QuiverDB::db::Db;

/// CLI: tde rotate — включить TDE и записать новую KID-эпоху.
///
/// Поведение (2.1):
/// - Если задан KEK в ENV (P1_KMS_KEK_HEX/P1_KMS_KEK_BASE64), генерируется новый DEK,
///   оборачивается через KMS (AES-GCM), сохраняется в <root>/keyring.bin под указанным KID,
///   затем epoch фиксируется в key_journal.bin.
/// - Если KEK не задан — fallback: ожидается ключ в EnvKeyProvider
///   (P1_TDE_KEY_HEX или P1_TDE_KEY_BASE64). В этом случае используется KID провайдера.
///   Если он отличается от переданного KID — явно сообщаем и используем провайдерский KID
///   для epoch и конфигурации (иначе ensure_tde_key упадёт).
///
/// Пример:
///   quiverdb tde-rotate --path ./db --kid mykid-v2
///
/// ENV (KMS):
///   P1_KMS_KEK_HEX / P1_KMS_KEK_BASE64 — 32-байтовый KEK
///   P1_KMS_KEK_KID — идентификатор KEK (по умолчанию "kek-default")
///
/// ENV (fallback):
///   P1_TDE_KEY_HEX / P1_TDE_KEY_BASE64 — 32-байтовый TDE ключ
///   P1_TDE_KID — KID для EnvKeyProvider (по умолчанию "default")
pub fn exec_rotate(path: PathBuf, requested_kid: String) -> Result<()> {
    // Откроем writer
    let mut db =
        Db::open(&path).with_context(|| format!("open writer DB at {}", path.display()))?;

    // with-KMS путь
    match EnvKmsProvider::from_env() {
        Ok(kms) => {
            // 1) Сгенерируем DEK 32 байта
            let mut dek = [0u8; 32];
            OsRng.fill_bytes(&mut dek);

            // 2) Завернём DEK под KEK (kid KEK берём у провайдера)
            let kek_kid = kms.default_kek_kid().to_string();
            let wrapped = kms
                .wrap(&kek_kid, &dek)
                .with_context(|| format!("kms wrap with KEK KID '{}'", kek_kid))?;

            // 3) Положим в KeyRing под запросный KID
            let kr = KeyRing::open_or_create(&path).context("open_or_create keyring.bin")?;
            kr.put(&requested_kid, &wrapped)
                .with_context(|| format!("keyring put kid='{}'", requested_kid))?;

            // 4) Включим TDE и выставим KID на уровень pager
            db.pager.set_tde_config(true, Some(requested_kid.clone()));

            // 5) ensure_tde_key() теперь раскроет DEK из KeyRing через KMS
            db.pager.ensure_tde_key().context(
                "ensure TDE key via KMS+KeyRing (set P1_KMS_KEK_HEX or P1_KMS_KEK_BASE64)",
            )?;

            // 6) epoch: эффективна с next LSN
            let since_lsn = db.pager.meta.last_lsn.saturating_add(1);
            let j = KeyJournal::open_or_create(&path)?;
            j.add_epoch(since_lsn, &requested_kid)
                .with_context(|| "add epoch to key_journal.bin")?;

            println!(
                "TDE rotate (KMS): KID='{}', KEK='{}', since_lsn={} (key stored in keyring, journal updated)",
                requested_kid, kek_kid, since_lsn
            );
            return Ok(());
        }
        Err(_) => {
            // Нет KEK — пойдём fallback-путём (EnvKeyProvider).
        }
    }

    // --- Fallback: EnvKeyProvider ---
    let provider = EnvKeyProvider::from_env().context(
        "EnvKeyProvider::from_env (set P1_TDE_KEY_HEX or P1_TDE_KEY_BASE64; optional P1_TDE_KID)",
    )?;
    let provider_kid = provider.default_kid().to_string();

    // Если пользователь указал KID и он отличается от KID провайдера, предупредим и используем провайдерский.
    let effective_kid = if provider_kid != requested_kid {
        eprintln!(
            "[WARN] Requested KID='{}' differs from EnvKeyProvider KID='{}'. \
             Using provider's KID to ensure consistency.",
            requested_kid, provider_kid
        );
        provider_kid.clone()
    } else {
        requested_kid.clone()
    };

    // Включаем TDE с эффективным KID и проверяем, что ключ загрузился
    db.pager.set_tde_config(true, Some(effective_kid.clone()));
    db.pager
        .ensure_tde_key()
        .context("ensure TDE key via EnvKeyProvider (set P1_TDE_KEY_HEX or P1_TDE_KEY_BASE64)")?;

    // epoch
    let since_lsn = db.pager.meta.last_lsn.saturating_add(1);
    let j = KeyJournal::open_or_create(&path)?;
    j.add_epoch(since_lsn, &effective_kid)?;

    println!(
        "TDE rotate (fallback): KID='{}', since_lsn={} (EnvKeyProvider; no keyring used)",
        effective_kid, since_lsn
    );

    Ok(())
}
