import io
import json

def data_to_stream_format(python_dict_data: dict) -> io.BytesIO:
    """
    Converte um dicionário Python em uma string JSON, depois em um objeto io.BytesIO para upload.
    """
    return (io.BytesIO(
        json.dumps(python_dict_data, separators=(',', ':'))
        .encode('utf-8'))
        .seek(0)
        )
        