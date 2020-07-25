use crate::utils;
use slog::{Drain, Level, Logger};

struct LevelFilter<D> {
    drain: D,
    level: String,
}

impl<D> Drain for LevelFilter<D>
where
    D: Drain,
{
    type Ok = Option<D::Ok>;
    type Err = Option<D::Err>;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        if record.level().is_at_least(level_filter(&self.level)) {
            self.drain.log(record, values).map(Some).map_err(Some)
        } else {
            Ok(None)
        }
    }
}

pub fn create_logger(id: u64, log_level: String) -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = LevelFilter {
        drain: drain,
        level: log_level,
    }
    .fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(4096)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .fuse();
    let logger = slog::Logger::root(drain, o!("tag" => format!("[{}]", id)));
    logger
}

fn level_filter(log_level: &str) -> Level {
    let log_level = utils::string_to_static_str(log_level.to_lowercase());
    match log_level {
        "debug" => Level::Debug,
        "info" => Level::Info,
        "warn" => Level::Warning,
        "error" => Level::Error,
        "critical" => Level::Critical,
        "trace" => Level::Trace,
        _ => Level::Info,
    }
}
