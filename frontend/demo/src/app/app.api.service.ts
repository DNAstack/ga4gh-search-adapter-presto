import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { AppConfigService } from './app-config.service';
import { MatSnackBar } from '@angular/material';
import { catchError, delay, map } from 'rxjs/operators';
import { ObservableInput } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ApiService {

  private readonly apiUrl;
  private readonly wesUrl;
  private readonly wepKcUrl = 'https://wep-keycloak.staging.dnastack.com/auth/realms/DNAstack/protocol/openid-connect/token';

  constructor(private httpClient: HttpClient,
    private app: AppConfigService,
    private snackBar: MatSnackBar) {
    this.apiUrl = app.config.apiUrl;
    this.wesUrl = app.config.wesUrl;
  }

  getFields() {
    return this.httpClient.get(`${this.apiUrl}/fields`)
      .pipe(
        catchError((err) => this.errorSnack(err))
      );
  }

  doQuery(query) {
    return this.httpClient.post(`${this.apiUrl}/search`, query)
      .pipe(
        catchError((err) => this.errorSnack(err))
      );
  }

  doWorkflowExecution(token, params) {
    const inputsJsonFileContents = `{"calculateMd5.input_file": ${params.vcf_object}}`;
    const file = new File([inputsJsonFileContents], 'inputs.json', {type: 'application/json'});
    const wepWorkflowUrl = 'https://wep-restapi.staging.dnastack.com/api/workflow/organization/303276/project/303293/workflow/303424';
    const wesRunsUrl = 'https://wes-translator.staging.dnastack.com/ga4gh/wes/v1/runs';

    let headers = new HttpHeaders();
    headers = headers
      .append('Authorization', `Bearer ${token}`);

    let data = new FormData();
    data.append('workflow_url', wepWorkflowUrl);
    data.append('workflow_params', file);

    return this.httpClient.post<any>(wesRunsUrl, data, {headers});
  }

  getToken(delayTime: number = 0) {
    let headers = new HttpHeaders();
    headers = headers.append("Content-Type", "application/x-www-form-urlencoded");
    const body = 'grant_type=password&client_id=dnastack-client&username=marc&password=changeit';

    return this.httpClient.post<any>(this.wepKcUrl, body, { headers}).pipe(
      delay(delayTime),
      map(({access_token}) => access_token)
    );
  }

  monitorJob(token, runId) {
    let headers = new HttpHeaders();
    headers = headers
      .append('Authorization', `Bearer ${token}`);
    return this.httpClient.get(`https://wes-translator.staging.dnastack.com/ga4gh/wes/v1/runs/${runId}/status`, {headers});
  }

  private errorSnack(err): ObservableInput<{}> {
    this.snackBar.open(err.message, null, {
      panelClass: 'error-snack'
    });
    throw err;
  }
}
