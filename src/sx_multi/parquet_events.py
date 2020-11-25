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
import awkward
import awkward1
from coffea.nanoaod import NanoEvents


class ParquetEvents(NanoEvents):
    def tolist(self):
        pass

    @classmethod
    def from_file(cls, file,  entrystart=None, entrystop=None, cache=None, methods=None, metadata=None):
        '''Build NanoEvents directly from ROOT file

        Parameters
        ----------
            file : str or uproot.rootio.ROOTDirectory
                The filename or already opened file using e.g. ``uproot.open()``
            treename : str, optional
                Name of the tree to read in the file, defaults to ``Events``
            entrystart : int, optional
                Start at this entry offset in the tree (default 0)
            entrystop : int, optional
                Stop at this entry offset in the tree (default end of tree)
            cache : dict, optional
                A dict-like interface to a cache object, in which any materialized virtual arrays will be kept
            methods : dict, optional
                A mapping from collection name to class deriving from `awkward.array.objects.Methods`
                that implements custom additional mixins beyond the defaults provided.
            metadata : dict, optional
                Arbitrary metadata to embed in this NanoEvents table

        Returns a NanoEvents object
        '''
        if cache is None:
            cache = {}

        array = awkward.from_parquet(file)
        arrays = {"Events": array}
        out = cls.from_arrays(arrays, methods=methods, metadata=metadata)
        out._cache = cache
        return out
