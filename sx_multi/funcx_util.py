from tenacity import retry, retry_if_exception, retry_if_exception_type, wait_fixed, wait_random
from funcx.sdk.utils.throttling import MaxRequestsExceeded


async def funcx_run_function_async(fxc, function_id: str, endpoint_id: str, *args, **kwargs):
    '''Run a single ROOT file through a coffea processor that has been installed on funcx.

    Arguments:
        function_id: The GUID returned from funcx for the function to run
        endpoint_id: The authorized funcx endpoint
        args: All arguments to be passed to the called function
        kwargs: The named arguments to be passed to the called function.

    Raises:
        funcx has a number of exceptions it might raise - we do our best to
        swallow and process the expected ones.

    Returns:
        Whatever the funcx guy returns.
    '''
    # Helper methods - lazy, so take advantage of lambda capture.
    @retry(wait=wait_fixed(3), retry=retry_if_exception_type(MaxRequestsExceeded))
    async def _submit_task():
        return fxc.run(function_id=function_id, endpoint_id=endpoint_id, *args, **kwargs)

    def _known_exception(e):
        'Return true if the exception is ok'
        return str(e) in ['waiting-for-ep', 'running', 'waiting-for-launch', 'waiting-for-nodes']

    @retry(retry=retry_if_exception_type(MaxRequestsExceeded) | retry_if_exception(_known_exception),
           wait=wait_fixed(3) + wait_random(0, 2))
    async def _fetch_results():
        return fxc.get_result(task_id)

    # #### Here the real logic of the method starts.
    task_id = await _submit_task()
    return await _fetch_results()