<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>Distillery</title>
    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
    <script>
        function validateCheckboxGroup() {
            var form_data = new FormData(document.querySelector("form"));
            if (!form_data.has("destinations")) {
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
        h1, h2 {
            margin-block: revert;
        }
        details {
            border: none;
            padding-block-start: var(--spacing);
        }
        #user [role=button] {
            white-space: nowrap;
        }
        input[type=text] {
            margin-block-end: revert;
        }
        :where(input) + small {
            margin-block-start: revert;
        }
        @media (min-width: 992px) {
            main > *:not(nav, details, h2) {
                margin-inline: 25%;
                max-width: 50%;
                padding-inline: var(--spacing);
            }
            main > nav#user, main > details {
                clear: right;
                float: right;
                width: 25%;
                padding-inline-start: var(--block-spacing-horizontal);
            }
            main > h2 {
                clear: left;
                float: left;
                max-width: 25%;
                padding-inline-end: var(--block-spacing-horizontal);
            }
        }
    </style>
</head>

<body>
    <main class="container">
        <nav id="user">
            <ul>
                <li>Hello, {{user["display_name"]}}!</li>
            </ul>
            <ul>
                <li><a href="{{distillery_base_url}}/Shibboleth.sso/Logout" role="button" class="secondary outline">Log out</a></li>
            </ul>
        </nav>
    <header>
        <h1>Distillery</h1>
    </header>
    <hr>
    <form action="{{distillery_base_url}}" method="post" onsubmit="return validateCheckboxGroup()">
        <h2>Collection</h2>
        <p>Enter a Collection ID as it appears in ArchivesSpace and the folder name on the filesystem.</p>
        <label>
            Collection ID
            <input type="text" name="collection_id" pattern="^[a-zA-Z0-9-\.~_]+$" title="only letters, numbers, hyphens, periods, tildes, and underscores allowed" required>
        </label>
        <div><small>examples: HaleGE, HBF, etc. (case-sensitive)</small></div>
        <div role="group" aria-labelledby="checkbox-group">
            <h2 id="checkbox-group">Destinations</h2>
            <p class="required">Select at least one destination for the available files.</p>
            <div class="checkboxes">
                <label>
                    <input type="checkbox" name="destinations" value="cloud">
                    Cloud preservation storage
                    <small>generate and send files to a remote storage provider</small>
                </label>
                <label>
                    <input type="checkbox" name="destinations" value="onsite">
                    On-site preservation storage
                    <small>generate and send files to a local tape drive</small>
                </label>
                <label>
                    <input type="checkbox" name="destinations" value="access">
                    Public web access
                    <small>generate files & metadata and publish on the web</small>
                </label>
            </div>
        </div>
        <input type="hidden" id="step" name="step" value="validate">
        <button>Continue</button>
    </form>
    </main>
</body>

</html>
