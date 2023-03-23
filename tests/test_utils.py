from pyfiles.utils import JobArgs


def test_job_args():
    job_args = JobArgs.parse_args([
        "--input_path", "/path/to/input",
        "--output_path", "/path/to/output"
    ])

    assert job_args.output_path == "/path/to/output"
    assert job_args.input_path == "/path/to/input"
