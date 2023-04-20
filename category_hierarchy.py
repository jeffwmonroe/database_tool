"""
This module category_hierarchy reads in a category_hierarchy file into
a n-tree.  This is super rough draft code which will either be deprecated
or integrated into the main code base
"""
# ToDo handle this code better
import pandas as pd

cat_adds = {}


class CategoryNode:
    def __init__(self, name):
        self.children = {}
        self.name = name

    def insert(self, value):
        if value not in self.children.keys():
            self.children[value] = CategoryNode(value)
            if value not in cat_adds.keys():
                cat_adds[value] = value
            else:
                print(f"Error duplicate entry: value {value} has already been added")
        return self.children[value]

    def insert_list(self, value_list):
        current_node = self
        for value in value_list:
            current_node = current_node.insert(value)

    def pprint_rec(self, level):
        pad = " " * (level * 3)
        if level >= 0:
            print(f"{pad} {self.name}")
        for value in self.children:
            self.children[value].pprint_rec(level + 1)

    def pprint(self):
        self.pprint_rec(-1)

    def add_to_df_rec(self, df, level):
        if level >= 0:
            new_row = ["", "", "", "", "", ""]
            new_row[level] = self.name
            df.loc[len(df.index)] = new_row
        for value in self.children:
            self.children[value].add_to_df_rec(df, level + 1)
        return df

    def add_to_df(self, df):
        df = self.add_to_df_rec(df, -1)
        return df

    def dump_to_html_rec(self, f):
        html_start = f'<li>\n<span class="tf-nc">{self.name}</span>\n'
        f.write(html_start)
        if True:  # self.name == 'base':
            if len(self.children.keys()) > 0:
                f.write("<ul>\n")
            for value in self.children:
                self.children[value].dump_to_html_rec(f)
            if len(self.children.keys()) > 0:
                f.write("</ul>\n")
        html_end = f"</li>"
        f.write(html_end)

    def dump_to_html(self):
        with open("hierarcy dump.html", "w") as f:
            html_start = """
<!DOCTYPE html>
    <html lang = "en">
    <head>
        <meta charset = "UTF-8">
        <title > Title </title>
        <link href = "https://unpkg.com/treeflex/dist/css/treeflex.css" rel = "stylesheet">
    </head>
    <body>
        <div class="tf-tree example">
        <ul>
        """
            f.write(html_start)
            self.dump_to_html_rec(f)
            html_end = """
        </ul>
    </div>
    </body>
</html>
        """
            f.write(html_end)


def process_category(category_df):
    num_to_name = {}
    name_to_num = {}
    for row in range(len(category_df.index)):
        num_to_name[category_df.iloc[row, 0]] = category_df.iloc[row, 1]
        name_to_num[category_df.iloc[row, 1]] = category_df.iloc[row, 0]
    return num_to_name, name_to_num


def main():
    # Define a dictionary containing employee data
    print("Category Hierarchy")
    category_df = pd.read_csv("category.csv")
    categories, categories_name_to_num = process_category(category_df)
    if True:
        category_hierarchy_df = pd.read_excel("category_hierarchy_raw.xlsx")
    else:
        category_hierarchy_df = pd.read_csv("category_hierarchy.csv")

    print(f"cat heir: {category_hierarchy_df}")

    base = CategoryNode("base")

    for row in range(len(category_hierarchy_df.index)):
        value = category_hierarchy_df.iloc[row, 1:]
        val_list = []
        for col in value.keys():
            if value[col] not in val_list:
                val_list.append(value[col])
        print(f"value = {val_list}")
        base.insert_list(val_list)

    base.pprint()

    cat_dict = {
        "cat_0": [],
        "cat_1": [],
        "cat_2": [],
        "cat_3": [],
        "cat_4": [],
        "cat_5": [],
    }
    df = pd.DataFrame(cat_dict)
    df = base.add_to_df(df)
    print(f"df = {df}")
    df.to_csv("hierarchy out.csv")

    base.dump_to_html()


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    main()
