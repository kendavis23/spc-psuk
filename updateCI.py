#update the Category and Include fields

def updateci(rc_df, ex_df, s_df): 

    df = s_df.copy()

    for index, row in df.iterrows():
        description = df.loc[index, 'Description']
        description = description.lower()
        for rc_index, rc_row in rc_df.iterrows():
            sub = rc_df.loc[rc_index, 'Substring']
            cat = rc_df.loc[rc_index, 'Category']
            if sub in description:
                df.loc[index, 'Category'] = cat
        for ex_index, ex_row in ex_df.iterrows():
            ex_sub = ex_df.loc[ex_index, 'Substring']
            if ex_sub in description:
                df.loc[index, 'Ops_Include'] = False

    return df
