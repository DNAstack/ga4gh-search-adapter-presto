import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {AppConfigService} from './app-config.service';

@Injectable({
  providedIn: 'root'
})
export class ApiService {

  private readonly apiUrl;

  constructor(private httpClient: HttpClient,
              private app: AppConfigService) {
    this.apiUrl = app.config.apiUrl;
  }

  getFields() {
    return this.httpClient.get(`${this.apiUrl}/fields?table=demo_view`);
  }

  doQuery(query) {
    return this.httpClient.post(`${this.apiUrl}/search`, query);
  }
}
