use clap::Parser;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

/// Resolve products from an input JSON file and print resolved JSON to stdout.
/// Pass "-" to read from stdin.
#[derive(Parser, Debug)]
#[command(name = "resolve-products", version, about)]
struct Cli {
    /// Path to input JSON file (use "-" for stdin)
    input: PathBuf,

    /// Pretty-print output JSON
    #[arg(long)]
    pretty: bool,
}

fn read_input(path: &PathBuf) -> io::Result<String> {
    if path.as_os_str() == "-" {
        let mut buf = String::new();
        io::stdin().read_to_string(&mut buf)?;
        Ok(buf)
    } else {
        let mut f = File::open(path)?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        Ok(buf)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let input_str = read_input(&cli.input)?;
    let input_bytes = input_str.as_bytes();

    let mut resolved = resolve_products::resolve_products(input_bytes)?;

    io::copy(&mut resolved, &mut io::stdout())?;

    Ok(())
}
