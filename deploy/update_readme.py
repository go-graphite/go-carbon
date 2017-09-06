import os

with open(os.path.join(os.path.dirname(__file__), "..", "README.md"), "r") as f:
    readme = f.read()

p1, p2 = readme.split("```toml\n")
p2, p3 = p2.split("\n```", 1)

with open(os.path.join(os.path.dirname(__file__), "go-carbon.conf"), "r") as f:
    conf = f.read()

with open(os.path.join(os.path.dirname(__file__), "..", "README.md"), "w") as f:
    f.write(p1 + "```toml\n" + conf + "\n```" + p3)
