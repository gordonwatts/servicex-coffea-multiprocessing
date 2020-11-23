from typing import Any, Dict
from coffea.nanoevents.schemas import BaseSchema
import urllib.parse


def _build_record_array(name: str, name_mapping: Dict[str, str], contents: Dict[str, Any], record_name: str) -> Dict[str, Any]:
    '''Build a record array using the mapping we've got from the contents.

    Args:
        nam_mapping (Dict[str, str]): The mapping of user variable to column in the contents
        contents (Dict[str, Any]): The contents of the array we are building into
    '''
    items = {
        v_name: contents[col_name]['content']
        for v_name, col_name in name_mapping.items()
    }
    record = {
        "class": "RecordArray",
        "contents": items,
        "form_key": urllib.parse.quote("!invalid," + name, safe=""),
        "parameters": {"__record__": record_name},
    }
    first = contents[next(iter(name_mapping.values()))]
    return {
        "class": first["class"],
        "offsets": first["offsets"],
        "content": record,
        "form_key": first["form_key"],
        "parameters": {},
    }


class auto_schema(BaseSchema):
    '''Build a schema
    '''
    def __init__(self, base_form: Dict[str, Any]):
        '''Create an auto schema by parsing the names of the incoming columns

        Notes:
            - There is a recursiveness to this defintion, as there is to any data structure,
              that is not matched with this. This should be made much more flexible. Perhaps
              with something like python type-hints so editors can also take advantage of this.

        Args:
            base_form (Dict[str, Any]): The base form of what we are going to generate a new schema (form) for.
        '''
        super().__init__(base_form)

        # Get the collection names - anything with a common name before the "_".
        contents = self._form['contents']
        collections = set(k.split("_")[0] for k in contents)

        output = {}
        for c_name in collections:
            mapping = {
                k.split('_')[1]: k
                for k in contents if k.startswith(f'{c_name}_')
            }

            # Build the new data model from this guy. Look at what is available to see if
            # we can build a 4-vector collection or just a "normal" collection.
            is_4vector = 'pt' in mapping \
                and 'eta' in mapping \
                and 'phi' in mapping \
                and 'mass' in mapping
            record = _build_record_array(c_name, mapping, contents, "PtEtaPhiMCandidate" if is_4vector else "NanoCollection")
            record["parameters"].update({"collection_name": c_name})
            output[c_name] = record

        self._form["contents"] = output
