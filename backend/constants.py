SHIFT_DEFINITIONS = [
    {
        "code": "S1",
        "group": "fixed_small",
        "name": "Ca sang 1",
        "start": "07:00",
        "end": "11:00",
    },
    {
        "code": "S2",
        "group": "fixed_small",
        "name": "Ca sang 2",
        "start": "11:00",
        "end": "15:00",
    },
    {
        "code": "S3",
        "group": "fixed_small",
        "name": "Ca chieu toi 1",
        "start": "15:00",
        "end": "19:00",
    },
    {
        "code": "S4",
        "group": "fixed_small",
        "name": "Ca chieu toi 2",
        "start": "19:00",
        "end": "22:00",
    },
    {
        "code": "M1",
        "group": "fixed_main",
        "name": "Ca chinh 1",
        "start": "07:00",
        "end": "15:00",
    },
    {
        "code": "M2",
        "group": "fixed_main",
        "name": "Ca chinh 2",
        "start": "15:00",
        "end": "22:00",
    },
    {
        "code": "FLEX",
        "group": "flexible",
        "name": "Ca linh hoạt",
        "start": "00:00",
        "end": "23:59",
    },
]

SHIFT_CODE_SET = {item["code"] for item in SHIFT_DEFINITIONS}
