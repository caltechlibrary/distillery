<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="utf-8">
    <title>Distillery</title>
    <style type="text/css">
        * {
            box-sizing: border-box;
        }
        html, body {
            margin: 0;
            padding: 0;
        }
        body {
            align-items: center;
            display: flex;
            flex-direction: column;
            justify-content: center;
            min-height: 100vh;
        }
    </style>
</head>

<body>
    <p>
        <div>Hello, {{user["display_name"]}}!</div>
        <div><a href="/Shibboleth.sso/Logout">log out</a></div>
    </p>
    <form action="/distilling" method="post">
        <p>Enter a Collection ID as it appears in ArchivesSpace and the folder name on the filesystem:</p>
        <label for="collection_id">Collection ID</label>
        <input id="collection_id" name="collection_id" type="text" pattern="^[a-zA-Z0-9-\.~_]+$" title="only letters, numbers, hyphens, periods, tildes, and underscores allowed" required>
        <div><small>examples: HaleGE, HBF, etc. (case-sensitive)</small></div>
        <p>Select the process you would like to run for the Collection:</p>
        <input type="radio" id="report" name="process" value="report" required disabled>
        <label for="report">Preview Report</label>
        <div><small>run a report about the files found that would be processed</small></div>
        <input type="radio" id="preservation" name="process" value="preservation" required>
        <label for="preservation">Preservation</label>
        <div><small>send the files to Glacier</small></div>
        <input type="radio" id="access" name="process" value="access">
        <label for="access">Access</label>
        <div><small>publish the files in Islandora</small></div>
        <input type="radio" id="preservation_access" name="process" value="preservation_access" required>
        <label for="preservation_access">Preservation & Access</label>
        <div><small>send the files to Glacier and publish the files in Islandora</small></div>
        <button>🚀</button>
    </form>
</body>

</html>
