#![cfg(test)]

fn run_test(input_file: &str, output_file: &str) {
    let input_path = format!("./samples/{input_file}");
    let output_path = format!("./samples/{output_file}");

    let input = std::fs::read_to_string(input_path).expect("Failed to read input file");
    let expected_output = std::fs::read_to_string(output_path).expect("Failed to read output file");

    let actual_output = crate::parse_from_str(&input);
    assert_eq!(
        actual_output, expected_output,
        "Output does not match for {}",
        input_file
    );
}

#[test]
fn test_measurements_1() {
    run_test("measurements-1.txt", "measurements-1.out");
}

#[test]
fn measurements_20() {
    run_test("measurements-20.txt", "measurements-20.out");
}

#[test]
fn measurements_10000_unique_keys() {
    run_test(
        "measurements-10000-unique-keys.txt",
        "measurements-10000-unique-keys.out",
    );
}

#[test]
fn measurements_boundaries() {
    run_test("measurements-boundaries.txt", "measurements-boundaries.out");
}

#[test]
fn measurements_complex_utf8() {
    run_test(
        "measurements-complex-utf8.txt",
        "measurements-complex-utf8.out",
    );
}

#[test]
fn measurements_dot() {
    run_test("measurements-dot.txt", "measurements-dot.out");
}

#[test]
fn measurements_rounding() {
    run_test("measurements-rounding.txt", "measurements-rounding.out");
}

#[test]
fn measurements_short() {
    run_test("measurements-short.txt", "measurements-short.out");
}

#[test]
fn measurements_shortest() {
    run_test("measurements-shortest.txt", "measurements-shortest.out");
}
