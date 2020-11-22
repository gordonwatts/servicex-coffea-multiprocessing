import inspect

import aiostream
from coffea.processor.processor import ProcessorABC


def run_coffea_processor(events_url, tree_name, accumulator,  proc):
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


async def stream_coffea_results(results_stream, coffea_processor: ProcessorABC):
    '''Return the accumulated accumulator, one at a time, as a stream.

    Arguments:
        - minio_stream: The stream that resolves to accumulator results.
        - coffea_processor: the processor that will do the work - we need
          a clean accumulator for this.

    Notes:
        - Returns a stream of results. The `postprocess` function of the
          processor is only run on the very last one.
    '''
    # Wait for all the data to show up
    async def inline_wait(r):
        'This could be inline, but python 3.6'
        return await r

    finished_events = aiostream.stream.map(results_stream,
                                           inline_wait,
                                           ordered=False)

    # Finall, accumulate!
    # There is an accumulate pattern in the aiostream lib
    output = coffea_processor.accumulator.identity()
    async with finished_events.stream() as streamer:
        async for results in streamer:
            output.add(results)
            yield output

    # We have to run post-process - so everything above could be, at some level,
    # wrong.
    yield coffea_processor.postprocess(output)
