import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Field } from './model/search/field';

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private apiURL = 'http://localhost:8080';

  constructor(private httpClient: HttpClient) {

  }

  getFields() {
    return this.httpClient.get(`${this.apiURL}/fields`);
  }

  doQuery(query) {
    return this.httpClient.post(`${this.apiURL}/search`, query);
  }
}
