# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import dill as pickle

from funcx import FuncXClient
from funcx.sdk.utils.throttling import MaxRequestsExceeded

from sx_multi.executor import Executor, run_coffea_processor
from tenacity import retry, wait_fixed, retry_if_exception_type


class FuncXExecutor(Executor):
    def __init__(self, endpoint_id, process_function="301f653b-40b6-449e-ad2e-e57d3aaa33cd"):
        self.fxc = FuncXClient(asynchronous=True)
        self.endpoint_id = endpoint_id
        self.process_function = process_function

    def run_async_analysis(self, file_url, tree_name, accumulator, process_func):
        if not self.process_function:
            self.process_function = self.fxc.register_function(run_coffea_processor)

        pickled_process_func = pickle.dumps(process_func)

        data_result = self.safe_run(file_url, tree_name,
                                    accumulator,
                                    pickled_process_func,
                                    function_id=self.process_function)

        # Pass this down to the next item in the stream.
        return data_result

    @retry(wait=wait_fixed(5), retry=retry_if_exception_type(MaxRequestsExceeded))
    def safe_run(self, file_url, tree_name, accumulator, proc, function_id):
        return self.fxc.run(file_url,
                            tree_name,
                            accumulator,
                            proc,
                            True,
                            endpoint_id=self.endpoint_id,
                            function_id=function_id)
