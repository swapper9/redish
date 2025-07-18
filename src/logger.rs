use log4rs::{
    append::{
        console::ConsoleAppender,
        rolling_file::{
            policy::compound::{
                roll::fixed_window::FixedWindowRoller,
                trigger::size::SizeTrigger,
                CompoundPolicy
            },
            RollingFileAppender,
        }
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};
use std::path::PathBuf;

pub(crate) fn init_logger() -> Result<(), Box<dyn std::error::Error>> {
    let log_dir = "logs";
    std::fs::create_dir_all(log_dir)?;

    let pattern = "{d(%Y-%m-%d %H:%M:%S %Z)} | {h({l}):5.5} | {t} | {f}:{L} | {m}{n}";

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build();

    let size_trigger = SizeTrigger::new(10 * 1024 * 1024);

    let roller_pattern = PathBuf::from(format!("{}/app.{{}}.log.gz", log_dir));
    let window_roller = FixedWindowRoller::builder()
        .base(1) 
        .build(roller_pattern.to_str().unwrap(), 10)?;

    let compound_policy = CompoundPolicy::new(
        Box::new(size_trigger),
        Box::new(window_roller)
    );

    let rolling_file = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build(format!("{}/app.log", log_dir), Box::new(compound_policy))?;

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("rolling_file", Box::new(rolling_file)))
        .build(
            Root::builder()
                .appender("stdout")
                .appender("rolling_file")
                .build(log::LevelFilter::Info),
        )?;

    log4rs::init_config(config)?;

    Ok(())
}
