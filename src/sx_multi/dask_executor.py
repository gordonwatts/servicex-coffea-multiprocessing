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
from dask.distributed import Client


class DaskExecutor:
    def __init__(self, client_addr):
        self.dask = Client(client_addr, asynchronous=True)
        print(self.dask.cluster)

    async def execute(self, analysis, datasource):
        result_file_stream = datasource.stream_result_file_urls()

        func_results = self.launch_analysis(result_file_stream,
                                            analysis.accumulator,
                                            analysis.process)

        # Wait for all the data to show up
        async def inline_wait(r):
            'This could be inline, but python 3.6'
            x = await r
            return x

        finished_events = aiostream.stream.map(func_results,
                                               inline_wait,
                                               ordered=False)
        # Finally, accumulate!
        # There is an accumulate pattern in the aiostream lib
        output = analysis.accumulator.identity()
        async with finished_events.stream() as streamer:
            async for results in streamer:
                print(results)
                output.add(results)
                yield output

    async def launch_analysis(self, result_file_stream, accumulator, process_func):
        def run_coffea_processor(events_url, tree_name, accumulator, proc):
            '''Process a single file from a tree via a coffea processor
            on the remote node.

            Arguments:
              events_url: a URL to a ROOT file that uproot4 can open
              tree_name: The tree in the ROOT file to use for our data
              proc: function
            '''
            # Since we execute remotely, explicitly include everything we need.
            import awkward1 as ak
            from coffea.nanoevents import NanoEventsFactory, BaseSchema

            # This in is amazingly important - the invar mass will fail silently without it.
            # And must be done in here as this function is shipped off to the funcx processor
            # on a remote machine/remote python environment.
            from coffea.nanoevents.methods import candidate
            ak.behavior.update(candidate.behavior)

            # Use NanoEvents to build a 4-vector
            events = NanoEventsFactory.from_file(
                file=str(events_url),
                treepath=f'/{tree_name}',
                schemaclass=BaseSchema,
                metadata={
                    'dataset': 'mc15x',
                    'filename': str(events_url)
                }
            ).events()

            # Next, do the work
            return proc(accumulator, events)

        tree_name = None
        async for sx_data in result_file_stream:
            file_url = sx_data['url']

            # Determine the tree name if we've not gotten it already
            if tree_name is None:
                with uproot4.open(file_url) as sample:
                    tree_name = sample.keys()[0]

            # Get a future that will actually process this. We don't await here,
            # we'll do that down-stream.
            # NOTE: If we knew the tree name ahead of time, this pattern would
            # be much simpler.
            # TODO: Fix this.
            data_result = self.dask.submit(run_coffea_processor,
                                           events_url=file_url,
                                           tree_name=tree_name,
                                           accumulator=accumulator,
                                           proc=process_func)

            # Pass this down to the next item in the stream.
            yield data_result
