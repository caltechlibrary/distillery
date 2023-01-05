<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>{{collection_id}} | Distillery</title>
    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
    <style type="text/css">
        #log {
            border-radius: var(--border-radius);
            height: 0;
            margin-block-end: var(--spacing);
            max-height: 50vh;
            width: 100%;
        }
        @media (min-width: 992px) {
            main > * {
                margin-inline: 25%;
                max-width: 50%;
                padding-inline: var(--spacing);
            }
            #log {
                padding-inline: revert;
            }
        }
    </style>
</head>

<body>
    <main class="container">
    <pre style="background-color:lightgray;">
        <span style="color:blue;">{{collection_id}}</span>
        <span style="color:blue;">{{timestamp}}</span>
        <span style="color:blue;">{{destinations}}</span>
    </pre>
    <iframe id="log" src="{{distillery_base_url}}/log/{{collection_id}}/{{timestamp}}"></iframe>
    <script>
      const id = setInterval(function () {
        let l = document.getElementById('log');
        let d = l.contentDocument;
        d.location.reload();
        console.log(d.body.innerText);
        if (d.body.innerText.endsWith('🟡')) {
          // stop reloading
          clearInterval(id);
        };
        l.addEventListener('load', function() {
          if (d.body.scrollHeight > 0) {
            // add 48px to account for height of emojis
            l.style.height = d.body.scrollHeight + 48 + 'px';
            // scroll to bottom
            this.contentWindow.scrollTo(0, d.body.scrollHeight);
          }
        });
      // reload every second
      }, 1000);
    </script>
    <form id="init" action="{{distillery_base_url}}" method="post">
        <input type="hidden" id="collection_id" name="collection_id" value="{{collection_id}}">
        <input type="hidden" id="timestamp" name="timestamp" value="{{timestamp}}">
        <input type="hidden" id="destinations" name="destinations" value="{{destinations}}">
        <input type="hidden" id="step" name="step" value="run">
        <div class="grid">
        <button>Initiate Processing 🚀</button>
        <div id="cancel"><a href="{{distillery_base_url}}">Cancel</a></div>
        </div>
    </form>
    </main>
</body>

</html>
