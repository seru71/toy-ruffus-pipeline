import unittest
from unittest.mock import Mock


class DrmaaWrapperTest(unittest.TestCase):

    mock_session = Mock
    job_scrip_directory = "/tmp"


    def test_run_job(self):
        from mnm_drmaa_wrapper import run_job

        run_job("echo Hello",
                job_script_directory='/tmp/drmaa',
                run_locally = False,
                session = self.mock_session)



if __name__ == '__main__':
    unittest.main()
