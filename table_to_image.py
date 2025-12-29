from PIL import Image, ImageDraw, ImageFont
from tabulate import tabulate

def table_to_png(
    headers: list[str],
    rows: list[list],
    output_path: str,
    title: str | None = None
):
    """
    Convert table data to PNG image
    """

    table_text = tabulate(
        rows,
        headers=headers,
        tablefmt="grid",
        floatfmt=".2f"
    )

    lines = table_text.split("\n")
    font = ImageFont.load_default()

    # Calculate image size
    char_width = 7
    char_height = 15
    width = max(len(line) for line in lines) * char_width + 40
    height = len(lines) * char_height + 60

    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)

    y = 20
    if title:
        draw.text((20, y), title, fill="black", font=font)
        y += 30

    for line in lines:
        draw.text((20, y), line, fill="black", font=font)
        y += char_height

    img.save(output_path)
