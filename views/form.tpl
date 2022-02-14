<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="utf-8">
    <title>Distillery</title>
    <script>
        function validateCheckboxGroup() {
            var form_data = new FormData(document.querySelector("form"));
            if (!form_data.has("processes")) {
                var strong = document.createElement("strong");
                strong.textContent = document.querySelector(".required").textContent;
                document.querySelector(".required").innerHTML = strong.outerHTML;
                document.querySelector(".required").style.color = "red";
                document.querySelector(".checkboxes").style.outline = "medium solid red";
                document.querySelector(".checkboxes").style["box-shadow"] = "0 0 0.5rem red";
                return false;
            }
        }
    </script>
    <style type="text/css">
        /* document layout */
        * {
            box-sizing: border-box;
        }
        html, body {
            margin: 0;
            padding: 0;
        }
        body, nav {
            align-items: center;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }
        body {
            min-height: 100vh;
        }
    </style>
    <style type="text/css">
        /* document styling */
        body {
            font-family: sans-serif;
        }
    </style>
    <style type="text/css">
        /* element spacing */
        .checkboxes {
            padding: 1rem;
        }
        .checkboxes label,
        .checkboxes small {
            display: block;
        }
        .checkboxes label {
            margin-block: 1rem;
        }
        .checkboxes label:first-of-type {
            margin-block-start: 0;
        }
        .checkboxes label:last-of-type {
            margin-block-end: 0;
        }
        button {
            margin-block: 1rem;
            padding-inline: 1rem;
        }
    </style>
</head>

<body>
    <h1>Distillery</h1>
    <nav>
        <div>Hello, {{user["display_name"]}}!</div>
        <div><a href="{{base_url}}/Shibboleth.sso/Logout">log out</a></div>
    </nav>
    <form action="{{base_url}}/distilling" method="post" onsubmit="return validateCheckboxGroup()">
        <h2>Collection</h2>
        <p>Enter a Collection ID as it appears in ArchivesSpace and the folder name on the filesystem.</p>
        <label>
            Collection ID
            <input type="text" name="collection_id" pattern="^[a-zA-Z0-9-\.~_]+$" title="only letters, numbers, hyphens, periods, tildes, and underscores allowed" required>
        </label>
        <div><small>examples: HaleGE, HBF, etc. (case-sensitive)</small></div>
        <div role="group" aria-labelledby="checkbox-group">
            <h2 id="checkbox-group">Processes to Run</h2>
            <p class="required">Select at least one process you would like to run on the available files.</p>
            <div class="checkboxes">
                <label>
                    <input type="checkbox" name="processes" value="report" disabled>
                    Report on available files
                    <small>preview file and metadata status</small>
                </label>
                <label>
                    <input type="checkbox" name="processes" value="cloud">
                    Send to cloud preservation storage
                    <small>generate and send files to S3 Glacier Deep Archive</small>
                </label>
                <label>
                    <input type="checkbox" name="processes" value="onsite">
                    Prepare for on-site preservation storage
                    <small>generate and structure files for tape storage</small>
                </label>
                <label>
                    <input type="checkbox" name="processes" value="access">
                    Publish files and metadata for access
                    <small>generate files and metadata and ingest into Islandora</small>
                </label>
            </div>
        </div>
        <button>🚀</button>
    </form>
</body>

</html>
