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
    details {
      border: none;
      padding-block-start: var(--spacing);
    }
    #user [role=button] {
      white-space: nowrap;
    }
    @media (min-width: 992px) {
      main > *:not(nav, details) {
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
    %if op == "upload":
    %if component_id == "error":
    <p>❌ only <code>docx</code> files are allowed at this time</p>
    %else:
    <p>✅ the <b>{{component_id}}.docx</b> file was uploaded</p>
    <ul>
      <li>the <a href="https://github.com/{{github_repo}}/blob/main/transcripts/{{component_id}}/{{component_id}}.md"><b>{{component_id}}.md</b> file in GitHub</a> should be available shortly</li>
      <li>an ArchivesSpace Digital Object record should be created for <a href="{{archivesspace_staff_url}}/search?q={{component_id}}">{{component_id}}</a></li>
    </ul>
    %end
    %end
    %if op == "publish":
    %if component_id == "all":
    <p>✅ all transcripts were set to be (re)published</p>
    %else:
    <p>✅ the <b>{{component_id}}</b> transcript was set to be published</p>
    <ul>
      <li>the <a href="{{oralhistories_public_base_url}}/{{component_id}}/{{component_id}}.html">HTML transcript</a> and its <a href="{{resolver_base_url}}/{{component_id}}">resolver link</a> should be available shortly</li>
      <li>ArchivesSpace Digital Object Components should be created for <a href="{{archivesspace_staff_url}}/search?q={{component_id}}">{{component_id}}</a></li>
    </ul>
    %end
    %end
    %if op == "update":
    %if component_id == "all":
    <p>✅ metadata for all <a href="https://github.com/{{github_repo}}/tree/main/transcripts">transcripts in GitHub</a> will be updated shortly</p>
    %else:
    <p>✅ metadata for the <a href="https://github.com/{{github_repo}}/blob/main/transcripts/{{component_id}}/{{component_id}}.md"><b>{{component_id}}.md</b> transcript in GitHub</a> will be updated shortly</p>
    %end
    %end
    %if component_id != "error":
    <ul>
      <li>any errors will be logged and sent to <i>{{user["email_address"]}}</i></li>
    </ul>
    %end
  </main>
</body>

</html>
