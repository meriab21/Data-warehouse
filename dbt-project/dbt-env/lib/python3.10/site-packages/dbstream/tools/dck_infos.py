import datetime
import uuid


def generate_dck_info(row_data, batch_id=None, id_info='dck'):
    result = []
    _now = datetime.datetime.now()
    for row in row_data:
        if not row.get(f'__{id_info}_id___'):
            row[f'__{id_info}_id___'] = str(uuid.uuid4())
        if not row.get(f'__{id_info}_inserted_at___'):
            row[f'__{id_info}_inserted_at___'] = _now
        if batch_id and not row.get(f'__{id_info}_batch_id___'):
            row[f'__{id_info}_batch_id___'] = batch_id
        result.append(row)
    return result
