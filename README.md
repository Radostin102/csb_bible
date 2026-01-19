# csb_bible

A python script to download the full CSB Bible from https://read.csbible.com/ and save it to a text file.

You can download the pre-generated file [here](CSB_Full_Bible.txt).

## Usage

Install packages:

```bash
pip install -r requirements.txt
```

Ensure you are in the root directory of the repository and run:

```bash
python src/bible.py
```

The output file will be saved in `src` (or whatever directory the Python script is in).

## Dependencies

- [Python](https://www.python.org/downloads/)
- [Packages](requirements.txt):
  - requests
  - beautifulsoup4
  - rich
