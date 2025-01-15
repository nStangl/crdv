# Generates queries that build a JSON representation for nested structures.
# Receives the views as arguments.

import argparse
import re

# to avoid "SyntaxError: f-string expression part cannot include a backslash" errors on older
# versions of Python
NL = '\n'

# First projections done to the views
baseProjectColumns = {
    r'^Map': lambda x: [f't{x}.id AS id{x}', f'(t{x}.data).key AS key{x}', f'(t{x}.data).value AS value{x}'],
    r'^Counter': lambda x: [f't{x}.id AS id{x}', f't{x}.data AS data'],
    r'^Set': lambda x: [f't{x}.id AS id{x}', f't{x}.data AS data'],
    r'^Register': lambda x: [f't{x}.id AS id{x}', f't{x}.data AS data'],
    r'^List': lambda x: [f't{x}.id AS id{x}', f't{x}.pos AS pos{x}', f't{x}.data AS data'],
}

# Right relation keys to join with the left relation (the key in left relation is always 'id') 
joinKeys = {
    r'Map(Aw|Rw)Mvr': lambda x: f'any((t{x}.data).value)',
    r'^Map(?!(Aw|Rw)Mvr)': lambda x: f'(t{x}.data).value',
    r'^Set': lambda x: f't{x}.data',
    r'RegisterMvr': lambda x: f'any(t{x}.data)',
    r'^Register(?!Mvr)': lambda x: f't{x}.data',
    r'^List': lambda x: f't{x}.data',
}

# Columns to project at each level
projectColumns = {
    r'^Map': lambda x: [f'id{x}', f'key{x}'],
    r'^Set': lambda x: [f'id{x}'],
    r'^Register': lambda x: [f'id{x}'],
    r'^List': lambda x: [f'id{x}', f'pos{x}'],
}

# Aggregations that must applied at the root level
# (aggregate to map with maps, aggregate to sorted array with lists)
rootAggFunctions = {
    r'^Map': lambda x: [f'jsonb_object_agg(key{x}, value{x})'],
    r'^List': lambda x: [f'jsonb_agg(data ORDER BY pos{x})'],
    r'^Set': lambda x: [f'jsonb_agg(data)'],
}

# Functions to aggregate at each level
# (maps aggregate to map, sets and lists aggregate to arrays; mvr structures also aggregate to array)
aggFunctions = {
    r'Map(Aw|Rw)Mvr': lambda cols: [(f'jsonb_agg({cols[-1]})', len(cols) - 1), (f'jsonb_object_agg({cols[-2]}, {cols[-1]})', len(cols) - 2)],
    r'^Map(?!(Aw|Rw)Mvr)': lambda cols: [(f'jsonb_object_agg({cols[-2]}, {cols[-1]})', len(cols) - 2)],
    r'^Set': lambda cols: [(f'jsonb_agg({cols[-1]})', len(cols) - 1)],
    r'RegisterMvr': lambda cols: [(f'jsonb_agg({cols[-1]})', len(cols) - 1)],
    r'RegisterLww': lambda cols: [(f'(array_agg({cols[-1]}))[1]', len(cols) - 1)],
    r'^List': lambda cols: [(f'jsonb_agg({cols[-1]} ORDER BY {cols[-2]})', len(cols) - 2)]
}


# Retrieves the entry in the collection that matches the given type, by regex
def getEntry(collection, type):
    for name, entry in collection.items():
        if re.search(name, type):
            return entry

    raise Exception(f'Invalid view: {type}. Does the view exist and is it in a valid position?')


# Performs the base joins of all types.
# We use LATERAL joins to optimize the pushdown of join conditions to the computation of each view.
# Because each view is formed by complex operations (e.g., aggregations, filters, sorting), the
# planner often choses to compute each view separately and then join both tables using an hash or
# or merge join. As each view can return a large set of rows, this can take a long time, especially
# when we have a large number of joins.
# E.g.: T1 JOIN T2 ON T2.pk = T1.fk WHERE T1.pk = 'abc'
#       the planner can choose to build T1 (just a few rows, as is the regular case) and T2 (e.g, 1M 
#       rows) and only then join them, while its best to first filter T1 and use a nested loop + 
#       index scan on T2.pk to join both tables.
# Additionally, we also saw performance benefits even when the left relation returns a large amounts
# of rows, e.g., when we want join all rows of T1 with T2, namely thanks to the Memoize operator. 
def joins(types):
    lines = [f"{types[0]} AS t0"]

    for i in range(1, len(types)):
        lines.append(f"(SELECT * FROM {types[i]} WHERE id = {getEntry(joinKeys, types[i - 1])(i - 1)} OFFSET 0) AS t{i}")

    return ',\nLATERAL '.join(lines)


# Performs the base projection
def baseProject(types):
    columns = []

    for i, type in enumerate(types):
        lastTable = i == len(types) - 1
        if lastTable:
            columns.extend(getEntry(baseProjectColumns, type)(i))
        else:
            columns.extend(getEntry(baseProjectColumns, type)(i)[:-1])

    return ', '.join(columns)


# Performs the base aggregation
def rootAgg(types, query):
    try:
        getEntry(rootAggFunctions, types[-1])
    except:
        return query

    columns = []
    for j in range(len(types) - 1):
        columns.extend(getEntry(projectColumns, types[j])(j))
    columns.append(getEntry(projectColumns, types[len(types) - 1])(len(types) - 1)[0])
    aggFuncs = getEntry(rootAggFunctions, types[-1])(len(types) - 1)

    for func in aggFuncs:
        query = f'''SELECT {", ".join(columns)}, {func} AS data
FROM (
  {(NL + "  ").join(query.split(NL))}
) t
GROUP BY {", ".join([str(i + 1) for i in range(len(columns))])}'''

    return query


# Performs the aggregation at each level, to build the JSON representation
def agg(types, i, query):
    columns = []
    for j in range(i):
        columns.extend(getEntry(projectColumns, types[j])(j))
    columns.append('data')
    aggFuncs = getEntry(aggFunctions, types[i - 1])(columns)

    for func, ncols in aggFuncs:
        query = f'''SELECT {", ".join(columns[:ncols])}, {func} AS data
FROM (
  {(NL + "  ").join(query.split(NL))}
) t
GROUP BY {", ".join([str(i + 1) for i in range(ncols)])}'''

    return query


# Builds the query
def buildQuery(types):
    query = f'''SELECT {baseProject(types)}
FROM {joins(types)}'''

    query = rootAgg(types, query)

    for i in reversed(range(1, len(types))):
        query = agg(types, i, query)
        
    query = f'''SELECT id0 as id, data
FROM (
  {(NL + "  ").join(query.split(NL))}
) t'''

    return query


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('views', type=str, nargs='+', help='Nested views')
    parser.add_argument('-i', '--inline', help='Print the query inline', action='store_true', required=False)
    args = parser.parse_args()

    query = buildQuery(args.views)
    if args.inline:
        query = re.sub(r'\s+', ' ', query)
    
    print(query)


if __name__ == '__main__':
    main()
