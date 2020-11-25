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
from abc import ABC, abstractmethod
from typing import Callable, AsyncGenerator

import aiostream
import uproot4
from coffea.nanoaod import NanoEvents
from sx_multi.accumulator import Accumulator
from sx_multi.analysis import Analysis
from sx_multi.data_source import DataSource


class Executor(ABC):

    @abstractmethod
    def run_async_analysis(self, file_url: str, tree_name: str, accumulator: Accumulator, process_func: Callable):
        raise NotImplemented

    async def execute(self, analysis: Analysis, datasource: DataSource) -> AsyncGenerator[Accumulator, None]:
        """
        Launch an analysis against the given dataset on the implementation's task framework
        :param analysis:
            The analysis to run
        :param datasource:
            The datasource to run against
        :return:
            Stream of up to date histograms. Grows as each result is received
        """
        # Stream transformed file references from ServiceX
        result_file_stream = datasource.stream_result_file_urls()

        # Launch a task against this file
        func_results = self.launch_analysis_tasks_from_stream(result_file_stream,
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

    async def launch_analysis_tasks_from_stream(self, result_file_stream: AsyncGenerator[dict, None], accumulator: Accumulator, process_func: Callable):
        """
        Invoke the implementation's task runner on each file from the serviceX stream.
        We don't know the file's tree name in advance, so grab a sample the first time
        around to inspect the tree name
        :param result_file_stream:
        :param accumulator:
        :param process_func:
        :return:
        """
        tree_name = None
        async for sx_data in result_file_stream:
            file_url = sx_data['url']

            # Determine the tree name if we've not gotten it already
            if tree_name is None:
                with uproot4.open(file_url) as sample:
                    tree_name = sample.keys()[0]

            # Invoke the implementation's task launcher
            data_result = self.run_async_analysis(
                file_url=file_url,
                tree_name=tree_name,
                accumulator=accumulator,
                process_func=process_func)

            # Pass this down to the next item in the stream.
            yield data_result

    async def accumulate(self,
                         analysis: Analysis,
                         hist_stream: AsyncGenerator[Accumulator, None]) -> AsyncGenerator[Accumulator, None]:
        """
        Stream processor to accumulate histograms from each process task and yield the
        up-to-date accumulated histograms
        :param analysis: Analysis
            The analysis instance being accumulated
        :param hist_stream:  AsyncGenerator[Accumulator]
            Stream of histograms from each process task
        :return:
        """
        # There is an accumulate pattern in the aiostream lib
        output = analysis.accumulator.identity()
        async with hist_stream.stream() as streamer:
            async for results in streamer:
                print(results)
                output.add(results)
                yield output


def run_coffea_processor(events_url: str, tree_name: str,
                         accumulator: Accumulator,
                         proc: Callable[[Accumulator, NanoEvents],Accumulator],
                         explicit_func_pickle: bool = False) -> Accumulator:
    """
    Process a single file from a tree via a coffea processor on the remote node
    :param events_url:
        a URL to a ROOT file that uproot4 can open
    :param tree_name:
        The tree in the ROOT file to use for our data
    :param accumulator:
        Accumulator to store the results
    :param proc:
        Analysis fuction to execute. Must have signature
    :param explicit_func_pickle: bool
        Do we need to use dill to explicitly pickle the process function, or can we
        rely on the remote execution framework to handle it correctly?
    :return:
        Populated accumulator
    """
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

    output = accumulator.identity()
    if explicit_func_pickle:
        import dill as pickle
        f = pickle.loads(proc)
        return f(output, events)
    else:
        return proc(output, events)
