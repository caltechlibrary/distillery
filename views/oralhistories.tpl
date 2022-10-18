<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>Oral Histories | Distillery</title>
    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
    <style type="text/css">
        h1, h2 {
            margin-block: 0;
        }
        details {
            border: none;
            padding-block-start: var(--spacing);
        }
        #user [role=button] {
            white-space: nowrap;
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
        <details>
            <summary>External Components</summary>
            <ul>
                <li>archivesspace: <a href="{{archivesspace_staff_url}}">{{archivesspace_staff_url}}</a></li>
                <li>github: <a href="https://github.com/{{github_repo}}">{{github_repo}}</a></li>
                <li>s3: <a href="https://s3.console.aws.amazon.com/s3/buckets/{{s3_bucket}}">{{s3_bucket}}</a></li>
            </ul>
        </details>
        <header>
            <h1>Oral Histories</h1>
            <nav>
                <ul>
                    <li><a href="#add">Add</a></li>
                    <li><a href="#update">Update</a></li>
                    <li><a href="#publish">Publish</a></li>
                </ul>
            </nav>
        </header>
        <hr>
        <h2 id="add">Add</h2>
        <form action="{{distillery_base_url}}/oralhistories" method="post" enctype="multipart/form-data">
            <label for="file">Select a Microsoft Word <b>docx</b> file to upload:</label>
            <input type="file" name="file" id="file">
            <input type="submit" value="Upload">
        </form>
        <hr>
        <h2 id="update">Update Metadata</h2>
        <form action="{{distillery_base_url}}/oralhistories" method="post" enctype="multipart/form-data">
            <label for="component_id">Optionally enter a Component Unique Identfier for an ArchivesSpace record:</label>
            <input type="text" name="component_id" id="component_id" aria-describedby="optional_component_id">
            <p id="optional_component_id">If no Component Unique Identifier is entered, all metadata will be updated from ArchivesSpace.</p>
            <input type="submit" name="update" value="Update Metadata">
        </form>
        <hr>
        <h2 id="publish">Publish</h2>
        <form action="{{distillery_base_url}}/oralhistories" method="post" enctype="multipart/form-data">
            <p>Publish recent changes to the web:</p>
            <input type="submit" name="publish" value="Publish Changes">
        </form>
    </main>
</body>

</html>
