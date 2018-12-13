import string

communities = [
    "near north side",
    "loop",
    "near south side",
    "north center",
    "lake view",
    "lincoln park",
    "avondale",
    "logan square",
    "rogers park",
    "west ridge",
    "uptown",
    "lincoln square",
    "edison park",
    "norwood park",
    "jefferson park",
    "forest glen",
    "north park",
    "albany park",
    "ohare",
    "edgewater",
    "portage park",
    "irving park",
    "dunning",
    "montclare",
    "belmont cragin",
    "hermosa",
    "humboldt park",
    "west town",
    "austin",
    "west garfield park",
    "east garfield park",
    "near west side",
    "north lawndale",
    "south lawndale",
    "lower west side",
    "armour square",
    "douglas",
    "oakland",
    "fuller park",
    "grand boulevard",
    "kenwood",
    "washington park",
    "hyde park",
    "woodlawn",
    "south shore",
    "bridgeport",
    "greater grand crossing",
    "garfield ridge",
    "archer heights",
    "brighton park",
    "mckinley park",
    "new city",
    "west elsdon",
    "gage park",
    "clearing",
    "west lawn",
    "chicago lawn",
    "west englewood",
    "englewood",
    "chatham",
    "avalon park",
    "south chicago",
    "burnside",
    "calumet heights",
    "roseland",
    "pullman",
    "south deering",
    "east side",
    "west pullman",
    "riverdale",
    "hegewisch",
    "ashburn",
    "auburn gresham",
    "beverly",
    "washington heights",
    "mount greenwood",
    "morgan park"
]

communities.sort()

fhand = open("chicago_community_options.txt", 'w')

for community in communities:
    community_capticalized = string.capwords(community)
    if community == "ohare":
        community_capticalized = "O'Hare"
    option = " " * 12 + "<option value=\"{}\">{}</option>".format(community, community_capticalized) + '\n'
    fhand.write(option)

fhand.close()
