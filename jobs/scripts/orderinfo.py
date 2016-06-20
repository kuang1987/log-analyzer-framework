self._result_list = []
for ele in self._query_list:
    map = {}
    map['merchant_order_id'] = ele['merchant_order_id']
    map['rec_name'] = ele['rec_name']
    self._result_list.append(map)
