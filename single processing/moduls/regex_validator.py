import re as rx
import pandas as pd
import random

def is_valid_regex(parameter, typ):
    regex_patterns = {
        "email": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        "CIN": r'^\d{8}$',
        "phone": r'^\d{8}$',
        "dob": r'^\d{4}-\d{2}-\d{2}$'
    }
    return bool(rx.match(regex_patterns.get(typ, ''), parameter))

def correct_email(email):
    if "@" not in email:
        if '.' in email:
            parts = email.split('.')
            index = random.randint(1, len(parts[0]) - 1)
            corrected_email = parts[0][:index] + '@' + parts[0][index:] + '.' + '.'.join(parts[1:])
            return corrected_email, "correctable: added @"
        else:
            return "invalid@example.com", "non-correctable"
    elif email.count("@") > 1:
        corrected_email = email.replace('@', '', email.count("@") - 1)
        return corrected_email, "correctable: removed extra @"
    elif not rx.search(r'\.[a-zA-Z]{2,}$', email):
        corrected_email = email + ".com"
        return corrected_email, "correctable: added .com"
    elif not email.replace('@', '').replace('.', '').isalnum():
        corrected_email = ''.join(c if c.isalnum() or c in '@.' else '_' for c in email)
        return corrected_email, "correctable: replaced non-alphanumeric"
    return "invalid@example.com", "non-correctable"

def correct_phone_number(phone_number):
    corrected_phone = rx.sub(r'\D', '', phone_number)
    return (corrected_phone, "correctable") if len(corrected_phone) == 8 else ("00000000", "non-correctable")

def correct_dob(dob):
    if rx.match(r'^\d{2}-\d{2}-\d{4}$', dob):
        return '-'.join(reversed(dob.split('-'))), "correctable"
    return "1970-01-01", "non-correctable"

def validate_and_correct_data(df):
    def apply_corrections(row):
        email, phone, dob = row['email'], row['phone_number'], row['date_of_birth']
        valid_email, email_status = (email, "valid") if is_valid_regex(email, "email") else correct_email(email)
        valid_phone, phone_status = (phone, "valid") if is_valid_regex(phone, "phone") else correct_phone_number(phone)
        valid_dob, dob_status = (dob, "valid") if is_valid_regex(dob, "dob") else correct_dob(dob)
        return valid_email, email_status, valid_phone, phone_status, valid_dob, dob_status
    
    corrections = df.apply(apply_corrections, axis=1, result_type='expand')
    df[['valid_email', 'email_correction_status', 'valid_phone', 'phone_correction_status', 'valid_dob', 'dob_correction_status']] = corrections

    correctable_condition = lambda x: any("correctable" in status for status in x)
    non_correctable_condition = lambda x: all("non-correctable" in status for status in x)
    invalid_entries_correctable = df[df[['email_correction_status', 'phone_correction_status', 'dob_correction_status']].apply(correctable_condition, axis=1)]
    invalid_entries_non_correctable = df[df[['email_correction_status', 'phone_correction_status', 'dob_correction_status']].apply(non_correctable_condition, axis=1)]

    return df, invalid_entries_correctable, invalid_entries_non_correctable
