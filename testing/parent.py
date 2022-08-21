import child
from pathlib import Path

if __name__ == "__main__":
    print(__file__)
    print("Hello, world from parent!")
    ch = child.test("Hello, world from child!")
