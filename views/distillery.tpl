<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
  <title>Distillery</title>
  <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
  <style type="text/css">
    html, body, main { height: 100%; }
    body > main { padding: calc(var(--block-spacing-vertical) / 2) 0; }
    main > footer {
      position: sticky;
      top: 100vh;
    }
    h1 {
      margin-block-start: 0;
      margin-block-end: revert;
    }
    h2 { margin-block: revert; }
    input[type=text] { margin-block-end: revert; }
    :where(input) + small { margin-block-start: revert; }
    details {border-block-end: none; }
    iframe {
      margin-block-end: var(--spacing);
      max-height: 40vh;
      width: 100%;
    }
    #cancel {
      display: flex;
      margin-block-end: var(--spacing);
    }
    #cancel > a {
      align-self: center;
      margin: auto;
    }
    #user [role=button] { white-space: nowrap; }
    @media (min-width: 992px) {
      main > * {
        margin-inline: 17%;
        max-width: 66%;
        padding-inline: var(--spacing);
        width: 100%;
      }
    }
  </style>
</head>

<body>
  <main class="container">
    <header>
      <h1>Distillery</h1>
    </header>
    <hr>
    % if step == "collecting":
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
      <input type="hidden" id="step" name="step" value="validating">
      <button>Validate</button>
    </form>
    % elif step == "validating":
    <p>Validating metadata, files, and destinations for <b>{{collection_id}}</b>.</p>
    % elif step == "running":
    <p>Processing metadata and files for <b>{{collection_id}}</b>.</p>
    % end
    % if step == "validating" or step == "running":
    <details>
      <summary>Details</summary>
      <iframe id="log" src="{{distillery_base_url}}/log"></iframe>
    </details>
    % if step == "validating":
    <form action="{{distillery_base_url}}" method="post">
      <input type="hidden" id="collection_id" name="collection_id" value="{{collection_id}}">
      <input type="hidden" id="destinations" name="destinations" value="{{destinations}}">
      <input type="hidden" id="step" name="step" value="running">
      <div class="grid">
        <button aria-busy="true" disabled>Validating…</button>
        <div id="cancel"><a href="{{distillery_base_url}}">Cancel</a></div>
      </div>
    </form>
    % elif step == "running":
    <div><a href="{{distillery_base_url}}">back to form</a></div>
    % end
    <script>
      const p = document.getElementsByTagName('p')[0];
      const log = document.getElementById('log');
      const button = document.getElementsByTagName('button')[0];
      const details = document.getElementsByTagName('details')[0];
      let id;

      function reloadIframe() {
        log.contentDocument.location.reload();
        let text = log.contentDocument.body.innerText;
        if (text.indexOf('🟡') != -1) {
          clearInterval(id);
          if (p) {
            // TODO 🐞 something here behaves unexpectedly in Chrome
            // the paragraph is updated before the 🟡 emoji is in the log
            console.log(p);
            p.innerHTML = p.innerHTML.replace("Validating", "✅ Validated");
            p.innerHTML = p.innerHTML.replace("Processing", "✅ Processed");
          }
          if (button) {
            button.innerHTML = "Run 🚀";
            button.setAttribute("aria-busy", false);
            button.disabled = false;
          }
        } else if (text.indexOf('❌') !== -1) {
          clearInterval(id);
          if (button) {
            button.innerHTML = "❌ Failure";
            button.setAttribute("aria-busy", false);
          }
        }
        log.style.height = log.contentDocument.body.scrollHeight + 48 + 'px';
        log.contentWindow.scrollTo(0, log.contentDocument.body.scrollHeight);
      }

      function updateIframe() {
        requestAnimationFrame(reloadIframe);
      }

      details.addEventListener('toggle', updateIframe);
      id = setInterval(updateIframe, 1000);
    </script>
    % end
    <hr>
    <footer>
      <nav id="user">
        <ul>
          <li>Logged in as: {{user["display_name"]}}</li>
        </ul>
        <ul>
          <li><a href="{{distillery_base_url}}/Shibboleth.sso/Logout" role="button" class="secondary outline">Log out</a></li>
        </ul>
      </nav>
    </footer>
  </main>
</body>

</html>
