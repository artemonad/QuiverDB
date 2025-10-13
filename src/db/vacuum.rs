//! db/vacuum — комбинированная операция обслуживания: компактация + очистка сиротских OVERFLOW.
//!
//! Состав операции:
//! 1) Компактация всех бакетов (tail‑wins без tombstone/expired), без переписывания больших данных:
//!    OVERFLOW placeholder сохраняется как есть.
//! 2) Очистка сиротских OVERFLOW‑цепочек (sweep_orphan_overflow), чтобы освободить неиспользуемые страницы.
//!
//! Форматы на диске не меняются.
//! Операция требует writer‑режима (эксклюзивного lock), т.к. создаёт новые страницы и
//! обновляет directory head’ы, а затем модифицирует free‑лист.
//!
//! Возвращаемая структура VacuumSummary содержит отчёт компактора и число освобождённых OVERFLOW‑страниц.

use anyhow::Result;

use super::core::Db;
use super::compaction::CompactSummary;

/// Сводка “вакуумной” операции: компактация + sweep сиротских OVERFLOW.
#[derive(Debug, Clone)]
pub struct VacuumSummary {
    /// Отчёт по компактации всех бакетов.
    pub compaction: CompactSummary,
    /// Сколько OVERFLOW‑страниц было освобождено sweep’ом.
    pub overflow_pages_freed: usize,
}

impl Db {
    /// Выполнить вакуум: компактация всех бакетов + очистка сиротских OVERFLOW‑страниц.
    ///
    /// Замечание: операция записи (writer‑режим).
    pub fn vacuum_all(&mut self) -> Result<VacuumSummary> {
        // 1) Компактация всех бакетов
        let comp = self.compact_all()?;

        // 2) Очистка сиротских OVERFLOW
        let freed = self.sweep_orphan_overflow()?;

        Ok(VacuumSummary {
            compaction: comp,
            overflow_pages_freed: freed,
        })
    }
}