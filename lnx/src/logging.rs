use std::str::FromStr;
use crate::Args;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

pub fn init_logging(args: &Args) -> Result<()> {
    let filter = EnvFilter::from_str(&args.log_level)
        .context("Parse log level")?;
    
    
    let builder = tracing_subscriber::fmt::fmt()
        .with_env_filter(filter)
        .with_ansi(!args.log_no_ansi);
    
    if args.log_json {
        builder.json().init();
    } else {
        builder.compact().init();
    }
    
    Ok(())
}
