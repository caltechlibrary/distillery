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
        <div><a href="/Shibboleth.sso/Logout">log out</a></div>
    </nav>
    <form action="/distilling" method="post" onsubmit="return validateCheckboxGroup()">
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
                    <input type="checkbox" name="processes" value="onsite">
                    Prepare for on-site storage
                    <div><small>generate and structure files for tape storage</small></div>
                </label>
            </div>
        </div>
        <!-- TODO change form to checkboxes
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
        -->
        <button>🚀</button>
    </form>
</body>

</html>
