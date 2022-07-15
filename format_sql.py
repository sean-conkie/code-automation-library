input = """select col1,
col2,
case
when a.current_flag = '1' then
true
when a.current_flag = '0' then
false
end current_flag
from table
left join `project.dataset.table`
on
    condition = condition
where 1 = 1"""


import copy
import re


def reformat_sql_query(input):
    working_string = copy.copy(input)
    # change query to single line
    working_string = re.sub(r"\n", " ", working_string, flags=re.IGNORECASE)
    working_string = re.sub(r"\s+", " ", working_string, flags=re.IGNORECASE)

    # identify any semi colon and split into multiple lines
    working_string = re.sub(r";", r";\n", working_string, flags=re.IGNORECASE)

    # change working_string into a list and loop it
    queries = working_string.split(r"\n")

    for query in queries:
        # split query into sections (select, from, where, group, order, union, except etc)
        query = re.sub(
            r"(from|where|group by|order by|union all|union distinct|except all|except distinct)",
            r"\n\1",
            query,
            flags=re.IGNORECASE,
        ).strip()
        query = query.split("\n")
        query_dict = {
            "select": [],
            "where": [],
            "group": [],
            "order": [],
            "union": [],
            "except": [],
            "from": [],
        }

        for i, section in enumerate(query):
            target = query_dict.get(section.split(" ")[0])
            target.append({"section": section, "index": i})

        for select in query_dict.get("select", []):
            select = formatselect(select.get("section"))


def formatselect(input: str) -> str:
    """
    > It takes a string, finds all the columns, and then formats them with an alias

    Args:
      input (str): The string to be formatted.

    Returns:
      the formatted SQL statement.
    """

    # format case statements
    input = re.sub(r",\s+(case)\s", r",\n\1 ", input, flags=re.IGNORECASE)
    input = re.sub(r"\s+(when)\s", r"\n  \1 ", input, flags=re.IGNORECASE)
    input = re.sub(r"\s+(then)\s", r" \1\n    ", input, flags=re.IGNORECASE)
    input = re.sub(r"\s+(else)\s", r"\n  \1\n    ", input, flags=re.IGNORECASE)
    input = re.sub(r"\s+(end)\s", r"\n\1 ", input, flags=re.IGNORECASE)

    # format columns
    input = re.sub(r",\s+", r",\n", input, flags=re.IGNORECASE)

    # put select to own line, will put back at end, makes
    # alias processing easier
    input = re.sub(r"^select", r"select\n", input, flags=re.IGNORECASE)

    # format alias
    m = re.findall(r"\b\w+\b\s\b\w+\b\s*(?:$|\n|\,)", input, re.IGNORECASE)
    if m:
        for match in m:
            col = re.search(r"^(\b\w+\b)", match, re.IGNORECASE).group()
            alias = re.search(r"(?P<alias>\b\w+\b)\W$", match, re.IGNORECASE).group(
                "alias"
            )
            pad = 52 - len(col)
            repl = f"{col} {alias.rjust(pad)}"
            input = re.sub(match, repl, input, flags=re.IGNORECASE)

    input = re.sub(r"\n", "\n       ", input, flags=re.IGNORECASE).strip()
    return input


def formatfrom(input: str) -> str:

    # separate onto newlines
    input = re.sub(
        r"\b(left|full|right|inner|cross|on|or)\b", r"\n\1 ", input, flags=re.IGNORECASE
    )
    input = [
        {
            "type": condition.split(" ")[0],
            "content": re.sub(
                r"\b(and)\b", r"\n\1 ", condition, flags=re.IGNORECASE
            ).split("\n"),
        }
        for condition in input
    ]

    for condition in input:
        if condition.get("type") == "or":
            for item in condition.get("content"):
                item = re.sub(r"\b(or)\b\s+", r"\1 (", condition, flags=re.IGNORECASE)
                if len(item) > 1:
                    item = re.sub(r"\b(or)\b", r"\1 (", condition, flags=re.IGNORECASE)

    return input


if __name__ == "__main__":
    print(reformat_sql_query(input))
