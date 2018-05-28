
import pandas as pd
import pandas.api.types as types

from Levenshtein.StringMatcher import distance


""" Find and replace """


def find_replace(dataframe: pd.DataFrame, column_name: str, to_replace, value) -> pd.DataFrame:
    dataframe[column_name].replace(to_replace, value, inplace=True)
    return dataframe


def find_replace_regex(dataframe: pd.DataFrame, column_name: str, to_replace: str, value: str) -> pd.DataFrame:
    if types.is_string_dtype(dataframe[column_name]):
        dataframe[column_name].replace(to_replace, value, inplace=True, regex=True)
    return dataframe


def find_replace_all(dataframe: pd.DataFrame, to_replace, value) -> pd.DataFrame:
    dataframe.replace(to_replace, value, inplace=True)
    return dataframe


""" Normalization """


def normalize(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    col = dataframe[column_name]
    if not types.is_numeric_dtype(col):
        return dataframe
    dataframe[column_name] = (col - col.min()) / (col.max() - col.min())
    return dataframe


def normalize_all(dataframe: pd.DataFrame) -> pd.DataFrame:
    func = {False: lambda col: col,
            True: lambda col: (col - col.min()) / (col.max() - col.min())}
    return dataframe.transform(lambda col: func[types.is_numeric_dtype(dataframe[col.name])](col))


""" Outliers """


def remove_outliers(dataframe: pd.DataFrame, column_name: str, outside_range: float) -> pd.DataFrame:
    col = dataframe[column_name]
    if not types.is_numeric_dtype(col):
        return dataframe
    return dataframe[(col - col.mean()).abs() <= (col.std() * outside_range)]


def remove_all_outliers(dataframe: pd.DataFrame, outside_range: float) -> pd.DataFrame:
    return dataframe[dataframe.apply(lambda col:
                                     not types.is_numeric_dtype(dataframe[col.name])
                                     or (col - col.mean()).abs() <= (outside_range * col.std())).all(axis=1)]


""" Empty fields """


def fill_empty_mean(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if not types.is_numeric_dtype(dataframe[column_name]):
        return dataframe
    dataframe[column_name] = dataframe[column_name].fillna(dataframe[column_name].mean())
    return dataframe


def fill_empty_median(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if not types.is_numeric_dtype(dataframe[column_name]):
        return dataframe
    dataframe[column_name] = dataframe[column_name].fillna(dataframe[column_name].median())
    return dataframe


def fill_empty_value(dataframe: pd.DataFrame, column_name: str, value) -> pd.DataFrame:
    dataframe[column_name] = dataframe[column_name].fillna(value)
    return dataframe


def fill_all_empty_mean(dataframe: pd.DataFrame) -> pd.DataFrame:
    func = {False: lambda col: col,
            True: lambda col: col.fillna(col.mean())}
    return dataframe.apply(lambda col: func[types.is_numeric_dtype(dataframe[col.name])](col))


def fill_all_empty_median(dataframe: pd.DataFrame) -> pd.DataFrame:
    func = {False: lambda col: col,
            True: lambda col: col.fillna(col.median())}
    return dataframe.apply(lambda col: func[types.is_numeric_dtype(dataframe[col.name])](col))


""" Discretization """


def discretize_equiwidth(dataframe: pd.DataFrame, column_name: str, nr_bins: int) -> pd.DataFrame:
    if types.is_numeric_dtype(dataframe[column_name]):
        dataframe[column_name] = pd.cut(dataframe[column_name], nr_bins).apply(str)
    return dataframe


def discretize_equifreq(dataframe: pd.DataFrame, column_name: str, nr_bins: int) -> pd.DataFrame:
    if types.is_numeric_dtype(dataframe[column_name]):
        dataframe[column_name] = pd.qcut(dataframe[column_name], nr_bins).apply(str)
    return dataframe


def discretize_ranges(dataframe: pd.DataFrame, column_name: str, boundaries: [float]) -> pd.DataFrame:
    col = dataframe[column_name]
    if types.is_numeric_dtype(col):
        boundaries.sort()
        if boundaries[0] < col.min() or boundaries[len(boundaries) - 1] > col.max():
            return dataframe

        boundaries.insert(0, col.min() - (col.max() - col.min()) / 1000)
        boundaries.insert(len(boundaries), col.max())

        dataframe[column_name] = pd.cut(col, boundaries).apply(str)

    return dataframe


""" One-hot-encoding """


def one_hot_encode(dataframe: pd.DataFrame, column_name: str, use_old_name: bool) -> pd.DataFrame:
    if types.is_string_dtype(dataframe[column_name]):
        encoded_frame = pd.get_dummies(dataframe[column_name])

        index = dataframe.columns.tolist().index(column_name)
        dataframe.drop(labels=column_name, axis=1, inplace=True)
        for i in range(encoded_frame.shape[1]):
            column = encoded_frame[encoded_frame.columns[i]]
            if use_old_name:
                dataframe.insert(loc=index + i, column=column_name + '_' + encoded_frame.columns[i], value=column)
            else:
                dataframe.insert(loc=index + i, column=encoded_frame.columns[i], value=column)
    return dataframe


""" Type changing """


def change_type(dataframe: pd.DataFrame, column_name: str, new_type: str) -> pd.DataFrame:
    col = dataframe[column_name]
    if new_type == 'string':
        dataframe[column_name] = col.astype(str)
    elif new_type == 'int':
        dataframe[column_name] = col.astype(float).transform(round).astype(int)
    elif new_type == 'float':
        dataframe[column_name] = col.astype(float)
    elif new_type == 'datetime':
        dataframe[column_name] = col.apply(pd.to_datetime)
    return dataframe


""" Extract from date/time """


def extract_from_datetime(dataframe: pd.DataFrame, column_name: str, to_extract: str) -> pd.DataFrame:
    col = dataframe[column_name]
    if types.is_datetime64_any_dtype(col):
        if to_extract == 'year':
            dataframe[column_name] = col.dt.year.astype(int, errors='ignore')
        elif to_extract == 'month':
            dataframe[column_name] = col.dt.month.astype(int, errors='ignore')
        elif to_extract == 'week':
            dataframe[column_name] = col.dt.weekofyear.astype(int, errors='ignore')
        elif to_extract == 'day':
            dataframe[column_name] = col.dt.day.astype(int, errors='ignore')
        elif to_extract == 'weekday':
            dataframe[column_name] = col.dt.day_name()
    return dataframe


""" Data deduplication """


def find_duplicates(dataframe: pd.DataFrame, column_name: str, threshold: int) -> dict:
    col: pd.Series = dataframe[column_name]
    if not types.is_string_dtype(col):
        return {}

    """
    Find all possible duplicates for each string in the column, and count for each string the number of occurences
    """
    duplicates: {str, {str}} = {}
    occurences: {str, int} = {}
    distances_sum: {str, int} = {}

    for i in range(col.size):
        str_1: str = col[i]

        # Skip if str_1 has been encountered already
        if str_1 in occurences:
            continue
        occurences[str_1] = 1

        for j in range(i + 1, col.size):
            str_2: str = col[j]

            # Skip and increase the occurence counter if str_2 is the same as str_1
            if str_1 == str_2:
                occurences[str_1] += 1
                continue
            # Skip if str_2 has been encountered already
            if str_2 in occurences:
                continue

            # Only use pairs for which the edit distance is below the threshold
            edit_distance: int = distance(str_1, str_2)
            if edit_distance <= threshold:
                # Initialize the duplicate lists if necessary
                if str_1 not in duplicates:
                    duplicates[str_1] = {str_1}
                    distances_sum[str_1] = 0
                if str_2 not in duplicates:
                    duplicates[str_2] = {str_2}
                    distances_sum[str_2] = 0
                # Add str_1 and str_2 to eachothers duplicate lists
                duplicates[str_1].add(str_2)
                duplicates[str_2].add(str_1)
                distances_sum[str_1] += edit_distance
                distances_sum[str_2] += edit_distance

    """
    Find the most probable duplicate for each string
    """
    neighbours: {str, [str]} = {}

    for str_1 in duplicates:
        # Iterate over each string and sort their neighbours
        def get_weight(string: str) -> (int, int, str):
            return occurences[string] * len(duplicates[string]), -distances_sum[string], string

        neighbours[str_1] = sorted(duplicates[str_1], key=get_weight, reverse=True)

    return neighbours


def replace_duplicates(dataframe: pd.DataFrame, column_name: str, to_replace: {str, str}, chain: bool) -> pd.DataFrame:
    col = dataframe[column_name]
    if not types.is_string_dtype(col):
        return dataframe

    if chain:
        new_dict: {str, str} = {}
        for string in to_replace:
            new_dict[string] = to_replace[string]
            while new_dict[string] in to_replace:
                new_dict[string] = to_replace[new_dict[string]]
                if new_dict[string] == string:
                    del new_dict[string]
                    break
        to_replace = new_dict

    for i in range(col.size):
        if col[i] in to_replace:
            col[i] = to_replace[col[i]]
        dataframe[column_name] = col

    return dataframe


""" Testing """


def print_dict(to_print: dict) -> None:
        for key in to_print:
            print(key, ' ' * (20 - len(key)), ': ', to_print[key], sep='')
        print('\n')


if __name__ == "__main__":

    df = pd.DataFrame([[10.0, pd.Timestamp(2000, 1, 1, 1, 10), 1, 1, 'ABC'],
                       [1.0, pd.Timestamp(2001, 2, 2, 2, 20), 2, 2, 'ABD'],
                       [0.0, pd.Timestamp(2002, 3, 3, 3, 30), 0, 3, 'DBC'],
                       [1.0, pd.Timestamp(2003, 4, 4, 4, 40), 3, 4, 'DEC'],
                       [1.0, pd.Timestamp(2003, 4, 4, 4, 40), 3, 5, 'ADC'],
                       [1.0, pd.Timestamp(2003, 4, 4, 4, 40), 3, 6, 'DEF'],
                       [1.0, pd.Timestamp(2003, 4, 4, 4, 40), 3, 7, 'ABC'],
                       [1.0, pd.Timestamp(2003, 4, 4, 4, 40), 3, 8, 'XYZ'],
                       [1.0, pd.Timestamp(2003, 4, 4, 4, 40), 3, 9, 'DEF'],
                       [1.0, pd.Timestamp(2003, 4, 4, 4, 40), 3, 10, 'ABC'],
                       [1.0, pd.Timestamp(2003, 4, 4, 4, 40), 3, 11, 'XYH'],
                       [2.5, None,                            4, 4, 'XYI']],
                      columns=['A', 'B', 'C', 'D', 'E'])
    df_test = pd.DataFrame([['Abomasnow',    'Beedrill'],
                            ['Chimchar',     'Dunsparce'],
                            ['Entei',        'Flareon'],
                            ['Geodude',      'Houndoom'],
                            ['Infernape',    'Jolteon'],
                            ['Klang',        'Lairon'],
                            ['Mesprit',      'Natu'],
                            ['Octillery',    'Pikachu'],
                            ['Rampardos',    'Quagsire'],
                            ['Staraptor',    'Torchic'],
                            ['Umbreon',      'Vullaby'],
                            ['Wailord',      'Xatu'],
                            ['Yanmega',      'Zebstrika'],

                            ['infernape',    'Flarion'],
                            ['Obamasnow',    'Nato'],
                            ['Test',         'Beeedril'],
                            ['test_',        'Torchic'],
                            ['AbomaSnow',    'Flareon']],
                           columns=['A', 'B'])

    print_dict(find_duplicates(df, 'E', 1))
    print_dict(find_duplicates(df, 'E', 2))
