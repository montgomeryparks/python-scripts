import ast
from pathlib import Path


def extract_tool_classes(source_code):
    """Extracts class names defined in the given source code."""
    tool_classes = []
    try:
        tree = ast.parse(source_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                tool_classes.append(node.name)
    except SyntaxError as e:
        print(f"Syntax error while parsing tool classes: {e}")
    return tool_classes


def process_code_and_imports(source_code):
    """
    Parses source code to extract standard imports and identify
    local 'src.utils' dependencies required by the script.
    """
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        return source_code, set(), set()

    lines = source_code.split("\n")
    lines_to_remove = set()
    extracted_imports = set()
    required_utils = set()

    # Only look at module-level elements
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            # Capture the exact line numbers to erase from the tool block
            end_lineno = node.end_lineno if node.end_lineno is not None else node.lineno
            for i in range(node.lineno - 1, end_lineno):
                lines_to_remove.add(i)

            import_text = "\n".join(lines[node.lineno - 1 : node.end_lineno])
            is_local = False

            # Check if this import references our local src.utils folder
            if isinstance(node, ast.ImportFrom):
                if node.module == "src.utils":
                    # Matches: from src.utils import auth, constants
                    for alias in node.names:
                        required_utils.add(alias.name)
                    is_local = True
                elif node.module and node.module.startswith("src.utils."):
                    # Matches: from src.utils.auth import get_portal_token
                    util_name = node.module.split(".")[2]
                    required_utils.add(util_name)
                    is_local = True
            elif isinstance(node, ast.Import):
                # Matches: import src.utils.auth
                for alias in node.names:
                    if alias.name.startswith("src.utils."):
                        util_name = alias.name.split(".")[2]
                        required_utils.add(util_name)
                        is_local = True

            # If it's a standard third-party or arcpy import, save it for the header
            if not is_local:
                extracted_imports.add(import_text)

    # Rebuild the code without ANY of the import statements
    cleaned_lines = [line for i, line in enumerate(lines) if i not in lines_to_remove]
    cleaned_code = "\n".join(cleaned_lines).strip()

    return cleaned_code, extracted_imports, required_utils


def build_toolboxes():
    src_dir = Path("src")
    toolboxes_dir = src_dir / "toolboxes"
    utils_dir = src_dir / "utils"
    dist_dir = Path("dist")

    if not dist_dir.exists():
        dist_dir.mkdir(parents=True)

    if not toolboxes_dir.exists():
        print(f"Toolboxes directory '{toolboxes_dir}' not found. Exiting.")
        return

    # Iterate through toolboxes
    for toolbox_dir in toolboxes_dir.iterdir():
        if not toolbox_dir.is_dir():
            continue

        toolbox_name = toolbox_dir.name
        output_path = dist_dir / f"{toolbox_name}.pyt"

        tool_code_blocks = []
        all_tool_classes = []
        toolbox_imports = set(["import arcpy", "import os", "import sys"])

        # A queue of utility modules required by this specific toolbox
        utils_queue = set()

        py_files = list(toolbox_dir.glob("*.py"))

        if not py_files:
            print(f"Skipping '{toolbox_name}': No .py files found.")
            continue

        # 1. Process tools in this toolbox and collect dependencies
        for py_file in py_files:
            with open(py_file, "r", encoding="utf-8") as f:
                code = f.read()
                all_tool_classes.extend(extract_tool_classes(code))

                cleaned_code, standard_imports, local_utils = process_code_and_imports(
                    code
                )
                tool_code_blocks.append(cleaned_code)
                toolbox_imports.update(standard_imports)
                utils_queue.update(local_utils)

        # 2. Recursively resolve required utility files
        resolved_utils = set()
        utils_code_blocks = []

        while utils_queue:
            util_name = utils_queue.pop()
            if util_name in resolved_utils:
                continue

            resolved_utils.add(util_name)
            util_file = utils_dir / f"{util_name}.py"

            if util_file.exists():
                with open(util_file, "r", encoding="utf-8") as f:
                    code = f.read()
                    cleaned_code, standard_imports, local_utils = (
                        process_code_and_imports(code)
                    )
                    utils_code_blocks.append(
                        f"# --- src/utils/{util_name}.py ---\n{cleaned_code}"
                    )
                    toolbox_imports.update(standard_imports)

                    # If this utility imports another utility, add it to the queue
                    utils_queue.update(local_utils - resolved_utils)
            else:
                print(
                    f"Warning: Utility '{util_name}.py' imported but not found in src/utils/"
                )

        # 3. Clean and assemble the final imports header
        final_imports = list(toolbox_imports)
        final_imports.sort()
        imports_header = "\n".join(final_imports)

        # 4. Assemble the blocks
        combined_utils_code = "\n\n".join(utils_code_blocks)
        combined_tools_code = "\n\n# --- NEXT TOOL ---\n\n".join(tool_code_blocks)

        # 5. Generate the wrapper
        classes_list_str = ", ".join(all_tool_classes)
        alias = toolbox_name.lower().replace(" ", "").replace("_", "").replace("-", "")

        toolbox_wrapper = f"""
# --- DYNAMIC TOOLBOX WRAPPER ---
class Toolbox(object):
    def __init__(self):
        self.label = "{toolbox_name}"
        self.alias = "{alias}"
        self.tools = [{classes_list_str}]
"""

        # 6. Assemble the final clean PYT file
        clean_final_content = f"{imports_header}\n\n{combined_utils_code}\n\n{combined_tools_code}\n{toolbox_wrapper}"

        # 7. Write out the distribution file
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(clean_final_content)

        print(
            f"Successfully built: {output_path} | Utilities injected: {list(resolved_utils)}"
        )


if __name__ == "__main__":
    build_toolboxes()
