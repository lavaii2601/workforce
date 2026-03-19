SHIFT_DEFINITIONS = [
    {
        "code": "S1",
        "name": "Ca sang 1",
        "start": "07:00",
        "end": "11:00",
    },
    {
        "code": "S2",
        "name": "Ca sang 2",
        "start": "11:00",
        "end": "15:00",
    },
    {
        "code": "S3",
        "name": "Ca chieu toi 1",
        "start": "15:00",
        "end": "19:00",
    },
    {
        "code": "S4",
        "name": "Ca chieu toi 2",
        "start": "19:00",
        "end": "22:00",
    },
]

SHIFT_CODE_SET = {item["code"] for item in SHIFT_DEFINITIONS}
