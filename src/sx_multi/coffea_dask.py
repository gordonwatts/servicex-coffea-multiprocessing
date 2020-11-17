from typing import Any, Callable
import uproot4
import aiostream
from coffea.processor.processor import ProcessorABC

from .coffea_processing import run_coffea_processor, stream_coffea_results


async def _coffea_sx_to_dask(minio_stream, coffea_processor: ProcessorABC,
                             dask_client):
    '''Given the minio stream, the processor function, and the accumulator - run everything on funcx.
    Return a copy what the processor returns.

    Arguments:
        minio_stream:  The stream of info from servicex that contains the minio url for
                    the ROOT files that are output from servicex.
        coffea_processor: Function pointer to the coffea processor that is self-contained
                        enough to run in the funcx environment. This must conform to the
                        api used for the `processor` in this notebook.
        accumulator:  The accumulator that we can use to store the histograms, etc., that
                    that we want to build.
        dask_client: dask client

    Returns:
        Sequence of the accumulator object, as each jobs fills. Note that this
        object is updated in place! So while python forces it to be thread safe,
        you don't get a history. The generator will terminate when all servicex and funcx
        jobs are finished.

    Notes:
        Processor has a particular API!! And must return just the results from that
        processor!
    '''
    # Loop over all incoming minio items
    tree_name = None
    async for sx_data in minio_stream:
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
        data_result = dask_client.submit(run_coffea_processor, events_url=file_url,
                                         tree_name=tree_name,
                                         proc=coffea_processor)

        # Pass this down to the next item in the stream.
        yield data_result


async def process_coffea_dask(minio_stream, coffea_processor: ProcessorABC,
                              dask_client):
    '''Return the accumulated accumulator, one at a time, as a stream.

    Arguments:

    Notes:

    '''
    func_results = _coffea_sx_to_dask(minio_stream, coffea_processor,
                                      dask_client)

    results = stream_coffea_results(func_results, coffea_processor)

    async for r in results:
        yield r
