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
import inspect

import aiostream
import uproot4
from funcx import FuncXClient
import dill as pickle

from sx_multi.executor import Executor, run_coffea_processor
from tenacity import retry, wait_fixed


class FuncXExecutor(Executor):
    def __init__(self, endpoint_id, process_function="10627493-4bdd-4690-b37e-0f6935aebe1b"):
        self.fxc = FuncXClient(asynchronous=True)
        self.endpoint_id = endpoint_id
        self.process_function = process_function
        print(self.fxc)

    async def execute(self, analysis, datasource):

        # Open a stream of event file URLs
        result_file_stream = datasource.stream_result_file_urls()

        # Open a stream of histograms from the analysis code
        func_results = self.launch_analysis(result_file_stream, analysis)

        # Little worker function to wait on each analysis to complete
        async def inline_wait(r):
            'This could be inline, but python 3.6'
            x = await r
            return x

        # Use an unordered map to return completed analysis tasks as they resolve
        finished_events = aiostream.stream.map(func_results,
                                               inline_wait,
                                               ordered=False)

        async for hist in self.accumulate(analysis, finished_events):
            yield hist

    async def launch_analysis(self, result_file_stream, analysis):
        tree_name = None
        function_id = self.fxc.register_function(run_coffea_processor)
        # function_id = self.process_function
        pickled_process_func = pickle.dumps(analysis.process)

        async for sx_data in result_file_stream:
            file_url = sx_data['url']

            # Determine the tree name if we've not gotten it already
            if not tree_name:
                with uproot4.open(file_url) as sample:
                    tree_name = sample.keys()[0]

            # Get a future that will actually process this. We don't await here,
            # we'll do that down-stream.
            # NOTE: If we knew the tree name ahead of time, this pattern would
            # be much simpler.
            # TODO: Fix this.
            data_result = self.safe_run(file_url, tree_name,
                                        analysis.accumulator,
                                        pickled_process_func,
                                        explicit_func_pickle=True,
                                        function_id=function_id)
            # Pass this down to the next item in the stream.
            yield data_result

    @retry(wait=wait_fixed(5))
    def safe_run(self, file_url, tree_name, accumulator, proc, function_id):
        return self.fxc.run(events_url=file_url,
                            tree_name=tree_name,
                            accumulator=accumulator,
                            proc=proc,
                            function_id=function_id,
                            endpoint_id=self.endpoint_id)
