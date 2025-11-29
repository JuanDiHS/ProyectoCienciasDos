from transport_opt.db.bplustree import BPlusTree

def test_insert_and_search():
    bpt = BPlusTree(order=4)
    bpt.insert("A", {"id":"A"})
    bpt.insert("B", {"id":"B"})
    assert bpt.search("A")["id"] == "A"
    assert bpt.search("B")["id"] == "B"
    assert bpt.search("C") is None