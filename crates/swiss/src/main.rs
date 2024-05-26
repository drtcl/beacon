use anyhow::Result;

fn main() -> Result<()> {
    let matches = swiss::get_cli().get_matches_from(wild::args());
    swiss::main_cli(&matches)
}
