use anyhow::Result;
use bpmpack::*;

fn main() -> Result<()> {
    let matches = args::get_cli().get_matches();
    cli_main(&matches)
}
