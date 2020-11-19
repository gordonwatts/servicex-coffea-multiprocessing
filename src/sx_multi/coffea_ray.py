import ray
import uproot4
from coffea.processor.processor import ProcessorABC

from sx_multi.coffea_processing import (run_coffea_processor,
                                        stream_coffea_results)


async def _coffea_sx_to_ray(minio_stream, coffea_processor: ProcessorABC):
    '''Given the minio stream, the processor function, and the accumulator - run everything on funcx.
    Return a copy what the processor returns.

    Arguments:
        minio_stream:  The stream of info from servicex that contains the minio url for
                    the ROOT files that are output from servicex.
        coffea_processor: Function pointer to the coffea processor that is self-contained
                        enough to run in the funcx environment. This must conform to the
                        api used for the `processor` in this notebook.

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
    r_func = ray.remote(run_coffea_processor)
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
        data_result = r_func.remote(events_url=file_url,
                                    tree_name=tree_name,
                                    proc=coffea_processor)

        # Pass this down to the next item in the stream.
        yield data_result


async def process_coffea_ray(minio_stream, coffea_processor: ProcessorABC):
    '''Return the accumulated accumulator, one at a time, as a stream.

    Arguments:

    Notes:

    '''
    # Get the stream of processes async identity results
    func_results = _coffea_sx_to_ray(minio_stream, coffea_processor)

    results = stream_coffea_results(func_results, coffea_processor)

    async for r in results:
        yield r
