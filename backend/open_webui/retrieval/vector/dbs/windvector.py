import subprocess
from typing import Optional

from open_webui.retrieval.vector.main import VectorItem, SearchResult, GetResult

import requests
import json


class WindVectorClient:
    def __init__(self):
        self.uri_prefix = "http://100.102.190.142:20998"

    def has_collection(self, collection_name: str) -> bool:
        # Check if the collection exists based on the collection name.
        # 对应调用接口007-showtable?tableName=test_tbl_0205_001
        # 当前逻辑是异常就返回NONE
        print(f"**my_log**:[start_has_collection]")
        url = f"{self.uri_prefix}/wind/vector/rest/v1/showtable"
        params = {'tableName': collection_name}
        print(f"**wind_log**:WindVectorClient has_collection,params={params}")
        response = requests.get(url=url, params=params)
        print(f"**wind_log**:WindVectorClient has_collection result:", response.text)
        if response.status_code == 200:
            text = json.loads(response.text)
            if text["code"] == 0:
                print(f"**my_log**:[end_has_collection_1]")
                return True
        print(f"**my_log**:[end_has_collection_2]")
        return False

    def delete_collection(self, collection_name: str):
        # Delete the collection based on the collection name.
        # 对应调用接口table/drop
        print(f"**my_log**:[start_delete_collection]")
        params = {'tableName': collection_name}
        print(f"**wind_log**:WindVectorClient delete_collection start,params={params}")
        url = f"{self.uri_prefix}/wind/vector/rest/v1/table/drop"
        response = requests.post(url=url, json=params,
                                 headers={"content-type": "application/json"},
                                 timeout=60)
        print(f"**wind_log**:WindVectorClient delete_collection result:", response.text)
        print(f"**my_log**:[end_delete_collection]")

    def _result_to_search_result(self, result) -> SearchResult:
        print(f"**my_log**:[start_result_to_search_result]")
        ids = []
        distances = []
        documents = []
        metadatas = []

        for item in result:
            _ids = []
            _distances = []
            _documents = []
            _metadatas = []

            _ids.append(item.get("nid"))
            _distances.append(item.get("score"))
            _documents.append(item.get("sentences")[0])
            _metadatas.append(item.get("metadata"))

            ids.append(_ids)
            distances.append(_distances)
            documents.append(_documents)
            metadatas.append(_metadatas)

        print(f"**my_log**:[end_result_to_search_result]")
        return SearchResult(
            **{
                "ids": ids,
                "distances": distances,
                "documents": documents,
                "metadatas": metadatas,
            }
        )

    def search(
            self, collection_name: str, vectors: list[list[float | int | str]], limit: int
    ) -> Optional[SearchResult]:
        # Search for the nearest neighbor items based on the vectors and return 'limit' number of results.
        # 看vectors参数，在query/v1 和query/v2接口中都找不到参数对应

        print(f"**my_log**:[start_search]")
        print(f"**wind_log**:WindVectorClient search start")
        url = f"{self.uri_prefix}/wind/vector/rest/v1/query"
        params = {
            "tableName": collection_name,
            "queryVector": vectors[0],
            "attributes": ["metadata"],
            "topk": limit
        }
        print(f"**wind_log**:WindVectorClient search params={params}")
        response = requests.post(url=url, json=params,
                                 headers={"content-type": "application/json"},
                                 timeout=60)
        print(f"**wind_log**:WindVectorClient search result:", response.text)
        print(f"**my_log**:[end_search]")
        return self._result_to_search_result(json.loads(response.text)["data"])

    def _result_to_get_result(self, result) -> GetResult:
        ids = []
        documents = []
        metadatas = []

        print(f"**my_log**:[start_result_to_get_result]")
        for item in result:
            _ids = []
            _documents = []
            _metadatas = []
            _ids.append(item.get("nid"))
            _documents.append(item.get("sentences")[0])
            _metadatas.append(item.get("metadata") if "metadata" in item.keys() else item.get("attribute_map"))

            ids.append(_ids)
            documents.append(_documents)
            metadatas.append(_metadatas)

        print(f"**my_log**:[end_result_to_get_result]")
        return GetResult(
            **{
                "ids": ids,
                "documents": documents,
                "metadatas": metadatas,
            }
        )

    def query(
            self, collection_name: str, filter: dict, limit: Optional[int] = None
    ) -> Optional[GetResult]:
        # Query the items from the collection based on the filter.
        # 类似于v1/query，filter的条件是and相连还是or相连，暂时看做and相连
        print(f"**my_log**:[start_query]")
        print(f"**wind_log**:WindVectorClient query start")
        url = f"{self.uri_prefix}/wind/vector/rest/v1/query"
        if limit is None:
            limit = 100
        filter_str = " and ".join(
            [
                f'metadata_{key} == {json.dumps(value)}'
                for key, value in filter.items()
            ]
        )
        params = {
            "tableName": collection_name,
            "queryVector": "",
            "attributes": ["metadata", "distance"],
            "filter": filter_str,
            "topk": limit
        }
        print(f"**wind_log**:WindVectorClient query params={params}")
        response = requests.post(url=url, json=params,
                                 headers={"content-type": "application/json"},
                                 timeout=60)
        print(f"**wind_log**:WindVectorClient query result:", response.text)
        if response.status_code == 200:
            resp_text = json.loads(response.text)
            if resp_text.get("code") == 0:
                data = json.loads(response.text)["data"]
                if len(data) > 0:
                    print(f"**my_log**:[end_query_1]")
                    return self._result_to_get_result(data)
                else:
                    print(f"**my_log**:[end_query_2]")
                    return None
            else:
                print(
                    f"**wind_log**:WindVectorClient query call interface return not zero code, params={params},response={resp_text}")
                print(f"**my_log**:[end_query_3]")
                return None
        else:
            print(f"**wind_log**:WindVectorClient query call interface failed, url=", url)
            print(f"**my_log**:[end_query_4]")
            return None

    def get(self, collection_name: str) -> Optional[GetResult]:
        # Get all the items in the collection
        # 要返回数据格式为{"ids":[],"documents":[],"metadatas": []}
        # 大概是调用2个接口的结合：v1/nids?tableName=test_tbl_0205_001 + 009-document_detail-nid
        get_nid_url = f"{self.uri_prefix}/wind/vector/rest/v1/nids"
        nids_response = requests.get(url=get_nid_url, params={'tableName': collection_name})
        nids_array = json.load(nids_response.text)["data"]

        print(f"**my_log**:[start_get]")
        result = []
        if len(nids_array) > 0:
            doc_detail_url = f"{self.uri_prefix}/wind/vector/rest/v1/document_detail"
            for nid in nids_array:
                params = {'tableName': collection_name, 'nid': nid}
                print(f"**wind_log**:WindVectorClient get parmas={params}")
                doc_detail_response = requests.get(url=doc_detail_url,
                                                   params={'tableName': collection_name, 'nid': nid})
                resp_json = json.load(doc_detail_response.text)

                if resp_json.get("code") == 0:
                    result.append(resp_json.get("data"))
            print(f"**my_log**:[end_get_1]")
            return self._result_to_get_result(result)
        print(f"**my_log**:[end_get_2]")

    def _create_collection(self, collection_name: str):
        params = {
            "tableName": collection_name,
            "splitTexts": "split_by_line_feed",
            "embedding": "Pangu-S-Text-Embedding-v1",
            "query_instruction_for_retrieval": ""
        }
        print(f"**my_log**:[start_create_collection]")
        print(f"**wind_log**:WindVectorClient _create_collection start")
        url = f"{self.uri_prefix}/wind/vector/rest/v1/db/create"
        response = requests.post(url=url, json=params,
                                 headers={"content-type": "application/json"},
                                 timeout=60)
        print(f"**wind_log**:WindVectorClient _create_collection result:", response.text)
        print(f"**my_log**:[end_create_collection]")

    def insert(self, collection_name: str, items: list[VectorItem]):
        # Insert the items into the collection, if the collection does not exist, it will be created.
        # 对应接口003-document/insert
        print(f"**my_log**:[start_insert]")
        print(f"**wind_log**:WindVectorClient insert start")
        if not self.has_collection(collection_name=f"{collection_name}"):
            self._create_collection(collection_name=collection_name)

        if len(items) > 0:
            params = {
                "tableName": collection_name,
                "documents": []
            }
            for item in items:
                json_item = {
                    "nid": item["id"],
                    "sentences": [item["text"]],
                    "metadata": item["metadata"],
                }
                for key, value in item["metadata"].items():
                    json_item[f"metadata_{key}"] = value
                params.get("documents").append(json_item)

            print(f"**wind_log**:WindVectorClient insert parmas={params}")
            url = f"{self.uri_prefix}/wind/vector/rest/v1/document/insert"
            response = requests.post(url=url, json=params,
                                     headers={"content-type": "application/json"},
                                     timeout=60)
            print(f"**wind_log**:WindVectorClient insert result:", response.text)
            print(f"**my_log**:[end_insert_1]")
        print(f"**my_log**:[end_insert_2]")

    def upsert(self, collection_name: str, items: list[VectorItem]):
        # Update the items in the collection, if the items are not present, insert them. If the collection does not exist, it will be created.
        print(f"**my_log**:[start_upsert]")
        print(f"**wind_log**:WindVectorClient upsert start")
        if not self.has_collection(collection_name=f"{collection_name}"):
            self._create_collection(collection_name=collection_name)

        if len(items) > 0:
            params = {
                "tableName": collection_name,
                "mode": 2,
                "documents": [
                    {
                        "nid": item["id"],
                        "sentences": [item["text"]],
                        "metadata": item["metadata"],
                    }
                    for item in items
                ]}
            print(f"**wind_log**:WindVectorClient upsert parmas={params}")
            url = f"{self.uri_prefix}/wind/vector/rest/v1/document/insert"
            response = requests.post(url=url, json=params,
                                     headers={"content-type": "application/json"},
                                     timeout=60)
            print(f"**wind_log**:WindVectorClient upsert result:", response.text)
            print(f"**my_log**:[end_upsert_1]")
        print(f"**my_log**:[end_upsert_2]")

    def delete(
            self,
            collection_name: str,
            ids: Optional[list[str]] = None,
            filter: Optional[dict] = None,
    ):
        # Delete the items from the collection based on the ids.
        # 对应接口011-documents/delete
        doc_delete_url = f"{self.uri_prefix}/wind/vector/rest/v1/documents/delete"
        print(f"**my_log**:[start_delete]")
        if ids:
            params = {"tableName": collection_name, "nids": ids}
            print(f"**wind_log**:WindVectorClient upsert ids parmas={params}")
            response = requests.post(url=doc_delete_url, json={"tableName": collection_name, "nids": ids},
                                     headers={"content-type": "application/json"},
                                     timeout=60)
            if json.load(response.text)["code"] == 0:
                print(f"**wind_log**:sucessfully deleted,table_name={collection_name},ids=", ids)
            print(f"**my_log**:[end_delete_1]")
        elif filter:
            # collection.delete(where=filter)
            filter_str = " and ".join(
                [
                    f'{key} == {json.dumps(value)}'
                    for key, value in filter.items()
                ]
            )
            print(f"**wind_log**:WindVectorClient upsert query filter parmas: collection_name={collection_name},filter={filter_str}")
            query_result = self.query(collection_name, filter)
            if query_result:
                query_result_nids = query_result.ids[0]
                params = {"tableName": collection_name, "nids": ids}
                print(f"**wind_log**:WindVectorClient upsert filter parmas={params}")
                response = requests.post(url=doc_delete_url,
                                         json={"tableName": collection_name, "nids": query_result_nids},
                                         headers={"content-type": "application/json"},
                                         timeout=60)
                if json.load(response.text)["code"] == 0:
                    print(f"**wind_log**:sucessfully deleted query nids,table_name={collection_name},ids=", query_result_nids)
                print(f"**my_log**:[end_delete_2]")
            print(f"**my_log**:[end_delete_3]")
        print(f"**my_log**:[end_delete_4]")


    def reset(self):
        # Resets the database. This will delete all collections and item entries.
        print(f"**my_log**:[start_reset]")
        clear_tables_url = f"{self.uri_prefix}/wind/vector/rest/v1/tables/clear"
        response = requests.post(url=clear_tables_url,
                                 headers={"content-type": "application/json"},
                                 timeout=60)
        if json.load(response.text)["code"] == 0:
            print(f"**wind_log**:sucessfully deleted all tables")
            print(f"**my_log**:[end_reset_1]")
        print(f"**my_log**:[end_reset_2]")
