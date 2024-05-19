import json
import re
import time
import datetime
import random
import decimal
import string
import hashlib
import base64
import zlib
import uuid
import fnmatch
from .app_state import AppState


class CommandUtilsManager:
    def __init__(self):
        self.app_state = AppState()
        self.json_util = JSONUtil()
        self.context_util = ContextUtil()
        self.expr_util = ExpressionUtil(self.context_util)
        self.query_util = QueryUtil(self.app_state, self.context_util)
        self.data_util = DataUtil()

    def prepare_json(self, result):
        return self.json_util.prepare_json(result)

    def get_context_from_key(self, data_store, key):
        return self.context_util.get_context_from_key(data_store, key)

    def replace_placeholders(self, arg, context):
        return self.context_util.replace_placeholders(arg, context)

    def evaluate_expression(self, expr, context=None):
        return self.expr_util.evaluate_expression(expr, context)

    def apply_function(self, func, arg):
        return self.expr_util.apply_function(func, arg)

    def handle_expression_functions(self, value, context):
        return self.expr_util.handle_expression_functions(value, context)

    def format_timestamp(self, format_type):
        return self.expr_util.format_timestamp(format_type)

    def eval_conditions_using_indices(self, conditions, indices, main_key, joins=None):
        return self.query_util.eval_conditions_using_indices(conditions, indices, main_key, joins)

    def process_joins(self, entry, join_table, join_key, indices):
        return self.query_util.process_joins(entry, join_table, join_key, indices)

    def fallback_full_data_scan(self, main_key, conditions, joins=None):
        return self.query_util.fallback_full_data_scan(main_key, conditions, joins)

    def eval_conditions(self, entries, conditions):
        return self.query_util.eval_conditions(entries, conditions)

    def eval_condition(self, entry, condition):
        return self.query_util.eval_condition(entry, condition)

    def eval_and_conditions(self, entry, and_condition):
        return self.query_util.eval_and_conditions(entry, and_condition)

    def eval_single_condition(self, entry, condition):
        return self.query_util.eval_single_condition(entry, condition)

    def compare_values(self, value, op, expected):
        return self.query_util.compare_values(value, op, expected)

    def parse_condition(self, condition):
        return self.query_util.parse_condition(condition)

    def parse_additional_args(self, args):
        return self.query_util.parse_additional_args(args)

    def parse_value_instructions(self, value):
        return self.data_util.parse_value_instructions(value)

    def parse_and_clean_fields(self, conditions):
        return self.query_util.parse_and_clean_fields(conditions)

    def filter_fields(self, data, include_fields, exclude_fields):
        return self.query_util.filter_fields(data, include_fields, exclude_fields)

    def parse_modifiers(self, conditions):
        return self.query_util.parse_modifiers(conditions)

    def extract_join_clauses(self, conditions):
        return self.query_util.extract_join_clauses(conditions)

    def apply_query_modifiers(self, combined_results, modifiers):
        return self.query_util.apply_query_modifiers(combined_results, modifiers)

    def custom_sort_key(self, x, order_by):
        return self.query_util.custom_sort_key(x, order_by)

    def find_and_remove_existing_index(self, field, identifier_value):
        return self.query_util.find_and_remove_existing_index(field, identifier_value)

    def set_nested_value(self, data, keys, value):
        return self.data_util.set_nested_value(data, keys, value)

    def get_nested_value(self, data, keys):
        return self.data_util.get_nested_value(data, keys)

    def delete_nested_key(self, data, keys):
        return self.data_util.delete_nested_key(data, keys)

    def format_as_list(self, data):
        return self.data_util.format_as_list(data)

    def cleanup_empty_dicts(self, data, path):
        return self.data_util.cleanup_empty_dicts(data, path)


class JSONUtil:
    @staticmethod
    def prepare_json(result):
        if isinstance(result, (dict, list)):  # If result is a dictionary or list, encode it as JSON
            return json.dumps(result)
        elif isinstance(result, str):
            try:
                # Check if the string is already valid JSON
                json.loads(result)
                return result  # It's already a JSON string, return as is
            except json.JSONDecodeError:
                return json.dumps(result)  # Not a JSON string, encode it
        else:
            return json.dumps(str(result))


class ContextUtil:
    @staticmethod
    def get_context_from_key(data_store, key):
        parts = key.split(':')
        current = data_store
        for part in parts[:-1]:
            current = current.get(part, {})
        return current

    @staticmethod
    def replace_placeholders(arg, context):
        placeholder_pattern = re.compile(r'%(\w+)')
        matches = placeholder_pattern.findall(arg)
        for match in matches:
            if match in context:
                arg = arg.replace(f'%{match}', str(context[match]))
            else:
                return f"ERROR: Placeholder {match} not found in context"
        return arg


class ExpressionUtil:
    def __init__(self, context_util):
        self.context_util = context_util

    def evaluate_expression(self, expr, context=None):
        context = context or {}
        # Updated regex to find the innermost function call
        func_pattern = re.compile(r"(\w+)\(([^()]*?)\)")

        while True:
            # Search for the innermost function call
            match = func_pattern.search(expr)
            if not match:
                break

            func, arg = match.group(1).upper(), match.group(2).strip()
            arg = self.context_util.replace_placeholders(arg, context)

            if "ERROR:" in arg:
                return arg

            result = self.apply_function(func, arg)
            if "ERROR:" in result:
                return result

            # Replace the full match (innermost function call) with its result
            expr = expr[:match.start()] + str(result) + expr[match.end():]

        return expr

    def apply_function(self, func, arg):
        try:
            if func == "BASE64":
                return base64.b64encode(arg.encode()).decode()
            elif func == "HASH":
                hasher = hashlib.sha256()
                hasher.update(arg.encode())
                return hasher.hexdigest()
            elif func == "MD5":
                md5_hasher = hashlib.md5()
                md5_hasher.update(arg.encode())
                return md5_hasher.hexdigest()
            elif func == "CHECKSUM":
                algo, value = arg.split(',', 1)
                algo = algo.strip().upper()
                value = value.strip()
                if algo == "CRC32":
                    return str(zlib.crc32(value.encode()) & 0xffffffff)
                elif algo == "SHA1":
                    sha1_hasher = hashlib.sha1()
                    sha1_hasher.update(value.encode())
                    return sha1_hasher.hexdigest()
                elif algo == "SHA256":
                    sha256_hasher = hashlib.sha256()
                    sha256_hasher.update(value.encode())
                    return sha256_hasher.hexdigest()
                else:
                    return "ERROR: Unsupported CHECKSUM algorithm"
            elif func == "RANDOM":
                return ''.join(random.choices(string.ascii_letters + string.digits, k=int(arg)))
            elif func == "UPPER":
                return arg.upper()
            elif func == "LOWER":
                return arg.lower()
            elif func == "UUID":
                return str(uuid.uuid4())
            elif func == "TIMESTAMP":
                timestamp_result = self.format_timestamp(arg)
                # Ensuring that the timestamp is returned as a string
                return str(timestamp_result) if isinstance(timestamp_result, int) else timestamp_result
            elif func == "ROUND":
                num, digits = arg.split(',')
                return str(round(float(num), int(digits)))
            elif func == "DECIMAL":
                num, decimals = arg.split(',')
                return str(decimal.Decimal(num).quantize(decimal.Decimal('1.' + '0' * int(decimals))))
            else:
                return f"ERROR: Unsupported function {func}"
        except Exception as e:
            return f"ERROR: {str(e)}"

    def handle_expression_functions(self, value, context):
        try:
            # Process the value, supporting nested function calls
            result = self.evaluate_expression(value, context)  # Pass context here
            return result
        except ValueError as e:
            return str(e)

    def format_timestamp(self, format_type):
        now = datetime.datetime.now()
        if format_type.lower() == 'unix':
            return int(now.timestamp())
        elif format_type.lower() == 'full':
            return now.replace(microsecond=0).isoformat()
        elif format_type.lower() == 'date':
            return now.date().isoformat()
        elif format_type.lower() == 'time':
            return now.time().replace(microsecond=0).isoformat()
        else:
            return "ERROR: Unknown TIMESTAMP format"


class QueryUtil:
    def __init__(self, app_state, context_util):
        self.app_state = app_state
        self.context_util = context_util

    def eval_conditions_using_indices(self, conditions, indices, main_key, joins=None):
        joins = joins or []
        conditions = conditions.replace("AND", "and").replace("OR", "or")  # Normalize for splitting
        condition_list = [cond.strip() for cond in re.split(r'\s+(and|or)\s+', conditions.strip())]
        results = []
        current_ids = set()
        current_logic = "and"

        index_missing = False
        value_missing = False

        for condition_part in condition_list:
            if condition_part in ["and", "or"]:
                current_logic = condition_part
                continue

            field, operation, value = self.parse_condition(condition_part)
            if not field or not operation or value is None:
                print("Error: Parsed condition is incorrect, check syntax.")
                return []

            nested_fields = field.split(':')
            index_level = indices.get(main_key)

            # Traverse through nested indices
            for nested_field in nested_fields:
                if isinstance(index_level, dict) and nested_field in index_level:
                    index_level = index_level[nested_field]
                else:
                    index_missing = True
                    break

            if index_missing:
                continue  # Skip to the next condition if index path is incorrect

            if 'values' in index_level:
                indexed_data = index_level['values']
                matching_ids = set()

                if index_level['type'] == 'string':
                    for key, ids in indexed_data.items():
                        if self.compare_values(key, operation, value):
                            matching_ids.update([id.split(':')[1] for id in ids.split(',')])
                elif index_level['type'] == 'set':
                    for key, ids in indexed_data.items():
                        if self.compare_values(key, operation, value):
                            matching_ids.update([id.split(':')[1] for id in ids])

                if not matching_ids:
                    value_missing = True

                if current_logic == "and":
                    if current_ids:
                        current_ids &= matching_ids
                    else:
                        current_ids = matching_ids
                elif current_logic == "or":
                    current_ids |= matching_ids

        if index_missing or (not current_ids and not value_missing):
            return self.fallback_full_data_scan(main_key, conditions, joins)

        final_results = []
        for data_id in current_ids:
            if data_id in self.app_state.data_store[main_key]:
                entry = {'key': data_id, **self.app_state.data_store[main_key][data_id]}
                for join_table, join_key in joins:
                    self.process_joins(entry, join_table, join_key, indices)
                final_results.append(entry)

        return final_results if final_results else []

    def process_joins(self, entry, join_table, join_key, indices):
        join_values = entry.get(join_key, [])

        if not isinstance(join_values, list):
            join_values = [join_values]

        joined_data = []
        processed_product_ids = set()

        if join_table in indices and join_key in indices[join_table]:
            join_index_info = indices[join_table][join_key]

            for join_value in join_values:
                join_value_str = str(join_value)
                if join_value_str in join_index_info['values']:
                    join_ids = join_index_info['values'][join_value_str]
                    if isinstance(join_ids, str):
                        join_ids = join_ids.split(',')

                    for jid in join_ids:
                        jid_key = jid.split(':')[-1]
                        if jid_key in self.app_state.data_store[join_table] and jid_key not in processed_product_ids:
                            product_entry = self.app_state.data_store[join_table][jid_key]
                            joined_data.append(product_entry)
                            processed_product_ids.add(jid_key)

        entry[join_table] = joined_data
        return entry

    def fallback_full_data_scan(self, main_key, conditions, joins=None):
        joins = joins or []
        data_to_query = self.app_state.data_store.get(main_key, {})

        if isinstance(data_to_query, dict):
            data_to_query = [{'key': k, **v} for k, v in data_to_query.items()]

        filtered_results = self.eval_conditions(data_to_query, conditions)

        final_results = []
        for entry in filtered_results:
            for join_table, join_key in joins:
                self.process_joins(entry, join_table, join_key, self.app_state.indices)
            final_results.append(entry)

        return final_results

    def eval_conditions(self, entries, conditions):
        results = [entry for entry in entries if self.eval_condition(entry, conditions)]
        return results

    def eval_condition(self, entry, condition):
        or_conditions = condition.split(' OR ')
        return any(self.eval_and_conditions(entry, or_cond.strip()) for or_cond in or_conditions)

    def eval_and_conditions(self, entry, and_condition):
        and_parts = and_condition.split(' AND ')
        return all(self.eval_single_condition(entry, cond.strip()) for cond in and_parts)

    def eval_single_condition(self, entry, condition):
        field, op, expected = self.parse_condition(condition)
        if field is None:
            print("Condition parsing failed.")
            return False

        value = entry
        entry_key = entry.get('key', 'Unknown Key')  # Assumes each entry has a 'key' to identify it
        for part in field.split(':'):
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return False

        return self.compare_values(value, op, expected)

    def compare_values(self, value, op, expected):
        if op == 'BETWEEN':
            return expected[0] <= float(value) <= expected[1]
        elif op in ['=', '!=']:
            if isinstance(value, str) or isinstance(expected, str):
                value, expected = str(value), str(expected)
            else:
                try:
                    value, expected = float(value), float(expected)
                except ValueError:
                    return False
        elif op in ['>', '>=', '<', '<=']:
            try:
                value, expected = float(value), float(expected)
            except ValueError:
                return False
        elif op == 'LIKE':
            value, expected = str(value).lower(), str(expected).lower()
            expected = expected.replace('%', '.*')
            return bool(re.match(f"^{expected}$", value))

        op_functions = {
            '=': lambda v, e: v == e,
            '!=': lambda v, e: v != e,
            '>': lambda v, e: v > e,
            '>=': lambda v, e: v >= e,
            '<': lambda v, e: v < e,
            '<=': lambda v, e: v <= e,
        }

        func = op_functions.get(op)
        if func:
            return func(value, expected)
        else:
            print(f"Unsupported operation {op}")
            return False

    def parse_condition(self, condition):
        if 'BETWEEN' in condition:
            field, values = condition.split('BETWEEN', 1)
            lower_bound, upper_bound = values.split(',', 1)
            return field.strip(), 'BETWEEN', (float(lower_bound.strip()), float(upper_bound.strip()))

        pattern = re.compile(r"([a-zA-Z0-9_:\[\]]+)\s*([=><!]+|LIKE)\s*['\"]?(.*?)['\"]?$", re.IGNORECASE)
        match = pattern.match(condition)
        if match:
            field, op, value = match.groups()
            return field, op, value.strip("'\"")
        return None, None, None

    def parse_additional_args(self, args):
        group_by, order_by, limit = None, None, None

        group_match = re.search(r'GROUPBY\((.*?)\)', args)
        order_match = re.search(r'ORDERBY\((.*?),(ASC|DESC)\)', args)
        limit_match = re.search(r'LIMIT\((\d+)(,\d+)?\)', args)

        if group_match:
            group_by = group_match.group(1).strip()

        if order_match:
            order_by = (order_match.group(1).strip(), order_match.group(2).strip())

        if limit_match:
            limit = [int(x) for x in limit_match.groups() if x is not None]

        clean_args = re.sub(r'GROUPBY\(.*?\)|ORDERBY\(.*?,(ASC|DESC)\)|LIMIT\(\d+(,\d+)?\)', '', args).strip()

        return clean_args, group_by, order_by, limit

    def parse_and_clean_fields(self, conditions):
        include_fields, exclude_fields, new_conditions = [], [], conditions
        if 'INCLUDE(' in conditions:
            include_fields = conditions.split('INCLUDE(')[1].split(')')[0].split(',')
            include_fields = [field.strip() for field in include_fields]
            new_conditions = conditions.replace('INCLUDE(' + ','.join(include_fields) + ')', '').replace('  ', ' ')
        if 'EXCLUDE(' in conditions:
            exclude_fields = conditions.split('EXCLUDE(')[1].split(')')[0].split(',')
            exclude_fields = [field.strip() for field in exclude_fields]
            new_conditions = conditions.replace('EXCLUDE(' + ','.join(exclude_fields) + ')', '').replace('  ', ' ')
        return include_fields, exclude_fields, new_conditions.strip()

    def filter_fields(self, data, include_fields=None, exclude_fields=None):
        def nested_get(data, path):
            """Get the value from a nested dictionary using a path."""
            keys = path.split(':')
            for key in keys:
                if not isinstance(data, dict):
                    return None
                data = data.get(key, None)
            return data

        def nested_set(data, path, value):
            """Set the value in a nested dictionary using a path."""
            keys = path.split(':')
            for key in keys[:-1]:
                data = data.setdefault(key, {})
            data[keys[-1]] = value

        def match_wildcard(pattern, key):
            """Check if a key matches a wildcard pattern."""
            return fnmatch.fnmatchcase(key, pattern)

        def apply_wildcards(data, result, wildcard_parts):
            """Apply wildcard matching to include fields."""
            if not wildcard_parts:
                return

            part = wildcard_parts[0]
            sub_parts = wildcard_parts[1:]

            if part == '*':
                for subkey, subvalue in data.items():
                    sub_result = result.setdefault(subkey, {})
                    if sub_parts:
                        apply_wildcards(subvalue, sub_result, sub_parts)
                    else:
                        result[subkey] = subvalue
            else:
                if part in data:
                    sub_result = result.setdefault(part, {})
                    if sub_parts:
                        apply_wildcards(data[part], sub_result, sub_parts)
                    else:
                        result[part] = data[part]

        def nested_filter(data, fields, include=True):
            """Filter the dictionary by including or excluding specified fields."""
            result = {}
            wildcard_fields = [field for field in fields if '*' in field]

            for key, value in data.items():
                if isinstance(value, dict):
                    sub_fields = [f[len(key)+1:] for f in fields if f.startswith(f"{key}:")]
                    result[key] = nested_filter(value, sub_fields, include)

                    # Apply wildcard exclusion/inclusion at the current level
                    for wf in wildcard_fields:
                        wf_parts = wf.split(':')
                        if match_wildcard(wf_parts[0], key):
                            sub_wf = ':'.join(wf_parts[1:])
                            if sub_wf:
                                result[key] = nested_filter(result.get(key, value), [sub_wf], include)
                            elif include:
                                result[key] = value
                            elif not include:
                                result.pop(key, None)
                else:
                    field_matches = any(match_wildcard(f, key) for f in fields)
                    if include and field_matches:
                        result[key] = value
                    elif not include and not field_matches:
                        result[key] = value

            return result

        if include_fields:
            result = {}
            non_wildcard_fields = [field for field in include_fields if '*' not in field]
            wildcard_fields = [field for field in include_fields if '*' in field]

            # Process non-wildcard fields
            for field in non_wildcard_fields:
                value = nested_get(data, field)
                if value:
                    nested_set(result, field, value)

            # Process wildcard fields
            for wf in wildcard_fields:
                wf_parts = wf.split(':')
                apply_wildcards(data, result, wf_parts)

            return result

        elif exclude_fields:
            result = nested_filter(data, exclude_fields, include=False)
            return result

        return data


    def parse_modifiers(self, conditions):
        modifiers = {
            'group_by': None,
            'order_by': None,
            'order_asc': True,
            'limit_start': 0,
            'limit_count': None
        }
        matches = re.findall(r'(GROUPBY|ORDERBY|LIMIT)\(([^)]+)\)', conditions)
        for match, value in matches:
            if match == 'GROUPBY':
                modifiers['group_by'] = value
            elif match == 'ORDERBY':
                parts = value.split(',')
                if len(parts) == 1:
                    modifiers['order_by'] = parts[0].strip()
                else:
                    modifiers['order_by'], direction = parts
                    modifiers['order_asc'] = direction.strip().upper() == 'ASC'
            elif match == 'LIMIT':
                limits = [int(x) for x in value.split(',') if x]
                if len(limits) == 1:
                    modifiers['limit_count'] = limits[0]
                elif len(limits) == 2:
                    modifiers['limit_start'], modifiers['limit_count'] = limits

        cleaned_conditions = re.sub(r'(GROUPBY|ORDERBY|LIMIT)\([^)]+\)', '', conditions).strip()
        return modifiers, cleaned_conditions

    def extract_join_clauses(self, conditions):
        joins = []
        join_matches = re.findall(r'JOIN\(\s*([^)]+)\s*\)', conditions)
        for join_match in join_matches:
            join_parts = join_match.split(',')
            joins.append((join_parts[0].strip(), join_parts[1].strip()))
            conditions = conditions.replace(f'JOIN({join_match})', '', 1)
        return joins, conditions

    def apply_query_modifiers(self, combined_results, modifiers):
        grouped_results = None

        if 'group_by' in modifiers and modifiers['group_by']:
            grouped_results = {}
            for entry in combined_results:
                if isinstance(entry, dict):
                    group_key = entry.get(modifiers['group_by'])
                    if group_key:
                        if group_key not in grouped_results:
                            grouped_results[group_key] = []
                        grouped_results[group_key].append(entry)

        if 'order_by' in modifiers and modifiers['order_by']:
            target = grouped_results.values() if grouped_results else [combined_results]
            for group in target:
                group.sort(key=lambda x: self.custom_sort_key(x, modifiers['order_by']),
                           reverse=not modifiers.get('order_asc', True))

        limit_start = int(modifiers.get('limit_start', 0))
        limit_count = modifiers.get('limit_count')
        if limit_count is not None:
            limit_count = int(limit_count)
            if grouped_results:
                for key in grouped_results:
                    grouped_results[key] = grouped_results[key][limit_start:limit_start + limit_count]
            else:
                combined_results = combined_results[limit_start:limit_start + limit_count]

        if grouped_results:
            return [grouped_results, combined_results]
        else:
            return combined_results

    def custom_sort_key(self, x, order_by):
        value = x.get(order_by, '')
        if isinstance(value, str):
            return (0, value.lower())
        elif isinstance(value, (int, float)):
            return (1, value)
        return (2, str(value))

    def find_and_remove_existing_index(self, field, identifier_value):
        for key, ids in self.app_state.indices.get(field, {}).items():
            if identifier_value in ids and len(ids) > 1:
                ids.remove(identifier_value)
                return key
            elif identifier_value in ids:
                del self.app_state.indices[field][key]
                return key
        return None


class DataUtil:
    @staticmethod
    def set_nested_value(data, keys, value):
        for key in keys[:-1]:
            if key not in data or not isinstance(data[key], dict):
                data[key] = {}
            data = data[key]
        data[keys[-1]] = value

        return data

    @staticmethod
    def get_nested_value(data, keys):
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            elif isinstance(data, list):
                try:
                    key_as_int = int(key)
                    data = data[key_as_int]
                except (ValueError, IndexError):
                    found = False
                    for item in data:
                        if isinstance(item, dict) and key in item:
                            data = item[key]
                            found = True
                            break
                    if not found:
                        return None
            elif isinstance(data, (str, int, float)):
                return None
            else:
                return None

        return data

    @staticmethod
    def delete_nested_key(data, keys):
        for key in keys[:-1]:
            data = data.get(key, {})
        if keys[-1] in data:
            del data[keys[-1]]

    @staticmethod
    def format_as_list(data):
        if isinstance(data, dict):
            formatted_list = []
            for k, v in data.items():
                if isinstance(v, dict):
                    if 'key' in v:
                        formatted_list.append({**v})
                    else:
                        formatted_list.append({'key': k, **v})
                else:
                    formatted_list.append({'key': k, 'value': v})
            return formatted_list
        elif isinstance(data, list):
            return data
        elif data is None:
            return []
        else:
            return [{'value': data}]

    @staticmethod
    def cleanup_empty_dicts(data, path):
        for i in range(len(path) - 1, 0, -1):
            current_level = data
            for part in path[:i]:
                current_level = current_level.get(part, None)
                if current_level is None:
                    return

            if not current_level.get(path[i], {}):
                del current_level[path[i]]
            else:
                break

    @staticmethod
    def parse_value_instructions(value):
        expiry = None

        if isinstance(value, str):
            expire_match = re.search(r'EXPIRE\((\d+)\)', value)
            if expire_match:
                expiry = time.time() + int(expire_match.group(1))
                value = re.sub(r'EXPIRE\(\d+\)', '', value)

            if value.startswith('{') and value.endswith('}'):
                try:
                    value_dict = json.loads(value)
                    if 'value' in value_dict:
                        value = value_dict['value']
                        if 'expiry' in value_dict and expiry is None:
                            expiry = time.time() + value_dict['expiry']
                except json.JSONDecodeError:
                    pass

        return value.strip(), expiry
