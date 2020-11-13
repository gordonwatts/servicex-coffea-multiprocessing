from typing import Any, Callable
import uproot4
import aiostream


async def _coffea_sx_to_dask(minio_stream, coffea_processor,
                             accumulator, dask_client):
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
        data_result = dask_client.submit(coffea_processor, events_url=file_url,
                                         tree_name=tree_name,
                                         accumulator=accumulator)

        # Pass this down to the next item in the stream.
        yield data_result


async def process_coffea_dask(minio_stream, coffea_processor: Callable[[str, str, Any], None],
                              accumulator, dask_client):
    '''Return the accumulated accumulator, one at a time, as a stream.

    WARNING: First time this is called in a session it will produce a URL and you'll need to
    put a token in. After that, in a session, it should just work silently.

    Arguments:

    Notes:

    '''
    # Get the stream of processes async identity results
    func_results = _coffea_sx_to_dask(minio_stream, coffea_processor,
                                      accumulator, dask_client)

    # Wait for all the data to show up
    async def inline_wait(r):
        'This could be inline, but python 3.6'
        return await r

    finished_events = aiostream.stream.map(func_results,
                                           inline_wait,
                                           ordered=False)

    # Finall, accumulate!
    # There is an accumulate pattern in the aiostream lib
    output = accumulator.identity()
    async with finished_events.stream() as streamer:
        async for results in streamer:
            output.add(results)
            yield output
