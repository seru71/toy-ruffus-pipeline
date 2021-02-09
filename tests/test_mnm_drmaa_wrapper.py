import drmaa
import unittest
from unittest.mock import Mock
import mnm_drmaa_wrapper
from mnm_drmaa_wrapper import submit_drmaa_job, run_job_using_drmaa


# reduce waiting times for tests
mnm_drmaa_wrapper.GEVENT_TIMEOUT_WAIT = 0
mnm_drmaa_wrapper.GEVENT_TIMEOUT_STARTUP = 0
mnm_drmaa_wrapper.JOBSTATUS_FAILED_TIMEOUT = 0



class SubmitDrmaaJobTests(unittest.TestCase):

    job_script_directory = "/tmp"
    exc_msg = "code 2: slurm_submit_batch_job error: Socket timed out on send/recv operation"

    def setUp(self) -> None:
        self.mock_logger = Mock()
        #self.mock_logger.debug.side_effect = lambda x : print("DEBUG:", x)
        #self.mock_logger.info.side_effect = lambda x: print("INFO:", x)

        self.mock_pipeline = Mock()
        self.mock_pipeline.is_job_suspended.return_value = False

        self.mock_session = Mock()
        self.mock_session.runJob.return_value=111111


    def test_no_exceptions_scenario(self) -> None:

        self.mock_session.jobStatus.side_effect = [drmaa.JobState.RUNNING, drmaa.JobState.DONE]

        jobid, _ = submit_drmaa_job("echo Hello", self.mock_session, None, self.mock_logger, self.mock_pipeline)

        self.mock_session.runJob.assert_called_once()
        self.assertEqual(self.mock_session.jobStatus.call_count, 2)
        self.assertEqual(jobid, self.mock_session.runJob.return_value)


    def test_job_status_2_exceptions(self) -> None:

        self.mock_session.jobStatus.side_effect = [ drmaa.errors.DrmCommunicationException(self.exc_msg),
                                                    drmaa.errors.DrmCommunicationException(self.exc_msg),
                                                    drmaa.JobState.RUNNING,
                                                    drmaa.JobState.DONE ]

        jobid, _ = submit_drmaa_job("echo Hello", self.mock_session, None, self.mock_logger, self.mock_pipeline)

        self.mock_session.runJob.assert_called_once()
        self.assertEqual(self.mock_session.jobStatus.call_count, 4)
        self.assertEqual(jobid, self.mock_session.runJob.return_value)

    def test_job_status_5_exceptions(self) -> None:

        self.mock_session.jobStatus.side_effect = [ drmaa.errors.DrmCommunicationException(self.exc_msg),
                                                    drmaa.errors.DrmCommunicationException(self.exc_msg),
                                                    drmaa.errors.DrmCommunicationException(self.exc_msg),
                                                    drmaa.errors.DrmCommunicationException(self.exc_msg),
                                                    drmaa.errors.DrmCommunicationException(self.exc_msg),
                                                    drmaa.JobState.RUNNING,
                                                    drmaa.JobState.DONE ]

        with self.assertRaises(drmaa.errors.DrmCommunicationException):
            submit_drmaa_job("echo Hello", self.mock_session, None, self.mock_logger, self.mock_pipeline)

        self.mock_session.runJob.assert_called_once()
        self.assertEqual(self.mock_session.jobStatus.call_count, 5)


class RunJobUsingDrmaaTests(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_logger = Mock()
        #self.mock_logger.debug.side_effect = lambda x : print("DEBUG:", x)
        #self.mock_logger.info.side_effect = lambda x: print("INFO:", x)

        self.mock_pipeline = Mock()
        self.mock_pipeline.is_job_suspended.return_value = False

        self.mock_session = Mock()
        self.mock_session.jobStatus.side_effect = [drmaa.JobState.RUNNING,
                                                   drmaa.JobState.DONE]

        self.mock_jobid = 111111
        exc_msg = "code 2: slurm_submit_batch_job error: Socket timed out on send/recv operation"
        self.mock_session.runJob.side_effect = [ drmaa.errors.DrmCommunicationException(exc_msg),
                                                 drmaa.errors.DrmCommunicationException(exc_msg),
                                                 self.mock_jobid ]

        submit_drmaa_job = Mock()



    def test_run_job_using_drmaa_failing_to_submit_resubmit_0(self) -> None:
        with self.assertRaises(drmaa.errors.DrmCommunicationException):
            run_job_using_drmaa("echo Hello",
                                drmaa_session=self.mock_session,
                                logger = self.mock_logger,
                                resubmit = 0,
                                pipeline = self.mock_pipeline)

        self.mock_session.runJob.assert_called_once()

    def test_run_job_using_drmaa_failing_to_submit_resubmit_2(self) -> None:

        with self.assertRaises(mnm_drmaa_wrapper.error_drmaa_job):
            run_job_using_drmaa("echo Hello",
                                drmaa_session=self.mock_session,
                                logger=self.mock_logger,
                                resubmit=2,
                                pipeline=self.mock_pipeline)

        self.assertEqual(2, self.mock_session.runJob.call_count)

    def test_run_job_using_drmaa_failing_to_submit_resubmit_5(self) -> None:
        run_job_using_drmaa("echo Hello",
                            drmaa_session=self.mock_session,
                            logger=self.mock_logger,
                            resubmit=5,
                            pipeline=self.mock_pipeline)

        self.assertEqual(3, self.mock_session.runJob.call_count)
        self.assertEqual(2, self.mock_session.jobStatus.call_count)




if __name__ == '__main__':
    unittest.main()
