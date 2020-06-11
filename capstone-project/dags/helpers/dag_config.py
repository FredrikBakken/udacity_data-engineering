def get_arguments(start_date):
    return {
        'owner': 'Fredrik Bakken',
        'start_date': start_date,
    }

def get_ds_time(ds):
    datestring = ds.split('-')
    year = datestring[0]
    month = datestring[1].lstrip("0")
    day = datestring[2].lstrip("0")

    return year, month, day
