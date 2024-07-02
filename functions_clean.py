import pandas as pd

def row_shift(df, name):
  """
  Function for shifting data that is misalaigned (like data thats in)
  Estimate row on Turkish immigrant data
  Note: name must be in lower case
  """
  for row_number in range(1, df.shape[0]):
    if name in str(df.iloc[row_number, 0]).lower():
      df.iloc[row_number - 1, 1:] = df.iloc[row_number, 1:]
  df = df[~df.iloc[:, 0].str.lower().str.contains(name)]
  df.reset_index(drop=True, inplace=True)
  return df

def re_label(df, new_labels):
  """
  changes all labels in df
  takes in list of strings return og data frame with new labels
  """
  df.columns = new_labels
  df = df[1:]
  df.reset_index(drop=True, inplace=True)
  return df

def col_shift(df, every_what, start_ind):
  """

  """
  cols_td = df.columns[start_ind::every_what]
  df = df.drop(columns=cols_td)
  return df

def col_drop_end(lastc_contains, df):
  last_ind = 0
  for i, label in enumerate(df.columns):
    if lastc_contains.lower() in str(label).lower():
      last_ind = i
      break
  df = df.drop(columns = df.columns[last_ind + 1:])
  return df

def rem_col_ex(to_keep, df):
  keep = []
  for index, label in enumerate(df.columns):
    if label in to_keep:
      keep.append(index)
  return keep

def move_first_column_to_end(df):

  # Get the current list of columns
  cols = list(df.columns)
    
  # Move the first column to the end
  cols.append(cols.pop(0))
    
  # Reindex the DataFrame with the new column order
  df = df.reindex(columns=cols)
  return df

def string_to_num(df, col_list):
  """
  takes in data frame and changes the value type form string to int so that
  it can be edited
  """
  for row_number in range(df.shape[0]):
    for val in col_list:
      df.iloc[row_number, val] = df.iloc[row_number, val].replace(",", "")
      df.iloc[row_number, val] = df.iloc[row_number, val].replace("±", "")
      df.iloc[row_number, val] = df.iloc[row_number, val].replace("%", "")
      if "." in str(df.iloc[row_number, val]):
        splt = df.iloc[row_number, val].split(".")
        l = len(splt[1])
        df.iloc[row_number, val] = int(splt[0]) + (int(splt[1]) / (10 ** l))   
      else:
        df.iloc[row_number, val] = int(df.iloc[row_number, val])
  return df

def num_clean(df, col_index):
  for row in range(df.shape[0]):
    df.iloc[row, col_index] = df.iloc[row, col_index][len(df.iloc[row, col_index]) - 5:]
  return df

def conv_fips(path):
  """
  converts txt fips file to a dictionary with the structure
  {state name: (county name, code)}
  """
  state_dict = {}
  with open(path, 'r') as file:
    current_key = None
    for line in file:
      line = line.strip()
      if len(line) == 0:
        continue
      if line[:5].isdigit():
        parts = line.split(maxsplit=1)
        # with how file is formated, parts[0] = code and parts[1] is state or county name
        if parts[0][2] == '0' and parts[0][3] == '0' and parts[0][4] == '0':
          current_key = parts[1].lower()
          state_dict[current_key] = []
        else:
            item = (parts[1].lower(), int(parts[0]))
            state_dict[current_key].append(item)
  return state_dict

def add_fips(df, fips_dict, col_name):
  """
  takes in fips_path as txt file and df, applies conv_fips so that
  the txt file becomes a dictionary and then adds it to the df 
  """
  df[col_name] = pd.Series(dtype = 'int')
  for row_number in range(0, df.shape[0]):
    #accessing correct fips code based on state
    str_split = str(df.iloc[row_number, 0]).split(",")
    state = str_split[len(str_split)-1]
    state = state[1:].lower()
    fips_code = 0
    for tup in fips_dict[state]:
      if str_split[0][:len(str_split)-9].lower() in tup[0]:
        fips_code = int(tup[1])
      continue
    #break after fips code is found for current row, adding fips code
    df.loc[row_number, col_name] = fips_code
  return df

def add_percent_col(df, col1_name, col2_name, new_name):
  df[new_name] = pd.Series(dtype = 'float')
  for row_number in range(0, df.shape[0]):
    if int(df.loc[row_number, col2_name]) != 0:
      df.loc[row_number, new_name] = ((int(df.loc[row_number, col2_name]) / int(df.loc[row_number, col1_name])) * 100)
    else:
      df.loc[row_number, new_name] = 0
  df[new_name] = df[new_name].round(2)
  return df

def add_percent_change_col(df, col1_name, col2_name, new_name):
  df[new_name] = pd.Series(dtype = 'float')
  for row_number in range(0, df.shape[0]):
    if int(df.loc[row_number, col2_name]) != 0:
      df.loc[row_number, new_name] = ((int(df.loc[row_number, col2_name])  - int(df.loc[row_number, col1_name]))/ int(df.loc[row_number, col1_name] ) * 100)
    else:
      df.loc[row_number, new_name] = 0
  df[new_name] = df[new_name].round(2)
  return df

def extract_row_bn(file_path, name):
  """
  Reads a CSV file and extracts the num+1th row (index num ).
  """
  df = pd.read_csv(file_path)
  for index in range(df.shape[0]):
    if name.lower() in str(df.iloc[index, 0]).lower():
      return df.iloc[index]
  return None

def comb_df(file_paths_list, name, years):
  """
  Takes in as many files as you want to check and combines them into one based
  off of 1 rows values 
  created to work with census data that have the city in the first row and other
  stuff in the label
  Input: 
    file_paths_list [list][strings]: list of file paths
    name [string]: name of row you want to combine by
    years [list][strings]: list of years that have been included to use as labels
  Output:
    df that has columns [val type, year1, year2,... yearN] with rows includng values
  """
  temp_Df = pd.read_csv(file_paths_list[0])
  labels = temp_Df.columns.tolist()
  first_row = temp_Df.iloc[0].tolist()
  new_df = pd.DataFrame(columns=labels) 
  new_df.loc[0] = first_row
 # Extract the specified row from each file and append to the new DataFrame
  for i, file_path in enumerate(file_paths_list):
    row = extract_row_bn(file_path, name)
    new_df.loc[i + 1] = row
  new_df = new_df.drop(new_df.columns[0], axis=1)
  new_df = new_df.transpose()
  col = ["Val Type"] + years
  new_df = new_df.set_axis(col, axis=1)
  new_df.reset_index(drop=True, inplace=True)
  return new_df

