"""Test setup for the processor
"""
import csv
import io
import processor

def test_line_parsing():
    """Test a correct CSV input that the keys are extracted properly
    """
    test_csv = """LMK_KEY,LODGEMENT_DATE,TRANSACTION_TYPE,TOTAL_FLOOR_AREA,ADDRESS,POSTCODE
a,b,c,d,e,f
"""
    test_dict = {'lmk_key': 'a', 'lodgement_date': 'b', 'transaction_type': 'c', 'total_floor_area': 'd', 'addtess': 'e', 'postcode': 'f'}
    reader = csv.DictReader(io.StringIO(test_csv), delimiter=',', quotechar='"')
    response = processor.parseRow(next(reader))
    for key in test_dict:
        assert test_dict[key] == response[key]

def test_incorrect_input_parsing():
    """Test an incorrect CSV input
    """
    test_csv = """ONE,TWO,THREE
1,2,3
"""
    reader = csv.DictReader(io.StringIO(test_csv), delimiter=',', quotechar='"')
    response = processor.parseRow(next(reader))
    assert response == None

if __name__ == "__main__":
    test_line_parsing()