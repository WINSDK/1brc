fn main() {
    let arg = std::env::args().skip(1).next().expect("Provide a path as an arg.");

    print!("{}", onebrc::parse_from_path(&arg));

    // prevent cleanup
    std::process::exit(0);
}
