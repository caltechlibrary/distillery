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
    <form action="/distilling" method="post">
        <label for="collection_id">CollectionID</label>
        <input id="collection_id" name="collection_id" type="text">
        <button>🚀</button>
    </form>
</body>

</html>
