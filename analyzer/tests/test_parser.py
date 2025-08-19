import pytest
from analyzer.parser import Parser

class TestParser():
    @staticmethod
    @pytest.mark.parametrize("file", ['test.json', 'test.csv'])
    def test_parser_return_success(mocker, file):
        mocked_pd_read_json = mocker.patch('analyzer.parser.pd.read_json')
        mocked_pd_read_csv = mocker.patch('analyzer.parser.pd.read_csv')

        Parser.parse(file)

        if file.endswith(".json"):
            mocked_pd_read_json.assert_called_once()
            mocked_pd_read_csv.assert_not_called()
        elif file.endswith(".csv"):
            mocked_pd_read_csv.assert_called_once()
            mocked_pd_read_json.assert_not_called()

    @staticmethod
    def test_parser_raises_exception_when_invalid_format():
        file = 'test.xml'
        
        with pytest.raises(Exception) as exc:
            Parser.parse(file)
        assert 'Not valid file format. Valid Formats [CSV, NDJSON]' in str(exc.value)