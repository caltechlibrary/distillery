<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
  <title>Oral Histories | Distillery</title>
  <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
  <style type="text/css">
    h1 {
      margin-block: 0;
    }
    #log {
      border-radius: var(--border-radius);
      height: 0;
      margin-block-end: var(--spacing);
      max-height: 50vh;
      width: 100%;
    }
    #user [role=button] {
      white-space: nowrap;
    }
    @media (min-width: 992px) {
      main > *:not(nav) {
        margin-inline: 25%;
        max-width: 50%;
        padding-inline: var(--spacing);
      }
      main > nav#user {
        clear: right;
        float: right;
        width: 25%;
        padding-inline-start: var(--block-spacing-horizontal);
      }
      #log {
        padding-inline: revert;
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
      <h1>Oral Histories</h1>
      <nav>
        <ul>
          <li><a href="{{distillery_base_url}}/oralhistories">Go back to the form.</a></li>
        </ul>
      </nav>
    </header>
    <hr>
    <iframe id="log" src="{{distillery_base_url}}/oralhistories/log/{{component_id}}/{{timestamp}}/{{op}}"></iframe>
    <script>
      const id = setInterval(function () {
        let l = document.getElementById('log');
        let d = l.contentDocument;
        d.location.reload();
        if (d.body.innerText.includes('🏁')) {
          // stop reloading
          clearInterval(id);
        }
        else if (d.body.innerText.includes('❌')) {
          // stop reloading
          clearInterval(id);
        };
        l.addEventListener('load', function() {
          if (l.contentDocument.body.scrollHeight > 0) {
            // add 48px to fit next item as it loads
            l.style.height = l.contentDocument.body.scrollHeight + 48 + 'px';
            // scroll to bottom
            this.contentWindow.scrollTo(0, l.contentDocument.body.scrollHeight);
          }
        });
      // reload every second
      }, 1000);
    </script>
  </main>
</body>

</html>
