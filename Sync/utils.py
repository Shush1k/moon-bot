def add_extra_data(records, bot_name, cur=None):
    res = []
    for i in records:
        if cur:
            res.append(tuple(i)+(bot_name,)+(cur,))
        else:
            res.append(tuple(i)+(bot_name,))
    return res
